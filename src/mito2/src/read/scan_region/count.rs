// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Optional exact-count preparation from manifest metadata.

use std::sync::Arc;

use common_time::range::TimestampRange;
use datafusion_common::ScalarValue;
use datafusion_expr::{Expr, Operator};
use datatypes::data_type::DataType;
use datatypes::value::Value;
use futures::FutureExt;
use futures::future::BoxFuture;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, build_primary_key_codec};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::ScanRequest;

use crate::compaction::run::files_overlap_inclusive;
use crate::read::scan_region::ScanInput;
use crate::sst::file::FileHandle;

/// Bound the work performed for each region scan.
const MAX_FILES: usize = 64;

/// Proves the complete scan row count or returns unknown.
pub(crate) fn prepare<'a>(
    input: &'a ScanInput,
    request: &'a ScanRequest,
) -> BoxFuture<'a, Option<usize>> {
    async move { count_files(&candidates(input, request)?) }.boxed()
}

/// Only time-index comparisons and leftmost primary key equalities combined
/// with AND are allowed, and every selected file must be wholly covered. File sources must be disjoint in
/// their combined inclusive time and primary key ranges.
fn candidates<'a>(input: &'a ScanInput, request: &ScanRequest) -> Option<Vec<&'a FileHandle>> {
    if input.compaction
        || input.files.len() > MAX_FILES
        || !input.memtables.is_empty()
        || input.series_row_selector.is_some()
        || input.sequence_range.is_some()
        || request.limit.is_some()
        || request.memtable_min_sequence.is_some()
        || request.memtable_max_sequence.is_some()
        || request.sst_min_sequence.is_some()
        || request.snapshot_on_scan
        || request.exact_sequence_range
        || request.skip_sst_files
    {
        return None;
    }
    #[cfg(feature = "enterprise")]
    if !input.extension_ranges.is_empty() {
        return None;
    }
    let count_filters = CountFilters::new(input.region_metadata(), &request.filters)?;
    let range = input.time_range.as_ref()?;
    let mut files = Vec::new();
    for file in &input.files {
        let (start, end) = file.time_range();
        if start > end {
            return None;
        }
        let file_range = TimestampRange::new_inclusive(Some(start), Some(end));
        if !range.intersects(&file_range) {
            continue;
        }
        if !range.contains(&start)
            || !range.contains(&end)
            || (input.region_partition_expr.is_some() && !input.should_skip_region_partition(file))
            || (!input.append_mode && !file.meta_ref().num_rows_is_logical)
            || !count_filters.covers(file)
        {
            return None;
        }
        files.push(file);
    }
    if !input.append_mode {
        files.sort_unstable_by_key(|file| file.time_range().0);
        let mapper = input.primary_key_mapper();
        for (index, file) in files.iter().enumerate() {
            let end = file.time_range().1;
            if files[index + 1..]
                .iter()
                .take_while(|other| other.time_range().0 <= end)
                .any(|other| {
                    file.raw_primary_key_range().is_none()
                        || other.raw_primary_key_range().is_none()
                        || files_overlap_inclusive(file, other, mapper)
                })
            {
                return None;
            }
        }
    }
    Some(files)
}

/// Static scan predicates a metadata count can absorb.
struct CountFilters {
    /// Values the leftmost primary key column must equal.
    leftmost_values: Vec<Value>,
    codec: Option<Arc<dyn PrimaryKeyCodec>>,
}

impl CountFilters {
    /// Returns `None` if any conjunct is neither a time-index comparison nor an
    /// equality between the leftmost primary key column and a literal.
    fn new(metadata: &RegionMetadata, filters: &[Expr]) -> Option<Self> {
        let time_index = &metadata.time_index_column().column_schema;
        let time_type = time_index.data_type.as_arrow_type();
        let mut conjuncts = Vec::new();
        for expr in filters {
            split_and(expr, &mut conjuncts);
        }
        let mut leftmost_values = Vec::new();
        for expr in conjuncts {
            if time_filter(expr, &time_index.name, &time_type) {
                continue;
            }
            let column = &leftmost_primary_key(metadata)?.column_schema;
            let value = eq_literal(expr, &column.name, &column.data_type.as_arrow_type())?;
            leftmost_values.push(Value::try_from(value.clone()).ok()?);
        }
        let codec = (!leftmost_values.is_empty()).then(|| build_primary_key_codec(metadata));
        Some(Self {
            leftmost_values,
            codec,
        })
    }

    /// Whether the leftmost key equalities hold for every row of the file.
    /// Keys are sorted memcomparable bytes, so when both bounds decode to the
    /// same leftmost value, every key between them shares it.
    fn covers(&self, file: &FileHandle) -> bool {
        let Some(codec) = &self.codec else {
            return true;
        };
        let Some((min, max)) = file.raw_primary_key_range() else {
            return false;
        };
        match (
            leftmost_value(&**codec, &min),
            leftmost_value(&**codec, &max),
        ) {
            (Some(low), Some(high)) => {
                low == high && self.leftmost_values.iter().all(|value| *value == low)
            }
            _ => false,
        }
    }
}

/// Decodes the first encoded primary key field. Dense `decode_leftmost`
/// currently returns the last field, so Dense keys are decoded in full.
fn leftmost_value(codec: &dyn PrimaryKeyCodec, key: &[u8]) -> Option<Value> {
    match codec.encoding() {
        PrimaryKeyEncoding::Sparse => codec.decode_leftmost(key).ok()?,
        PrimaryKeyEncoding::Dense => match codec.decode(key).ok()? {
            CompositeValues::Dense(values) => values.into_iter().next().map(|(_, value)| value),
            CompositeValues::Sparse(_) => None,
        },
    }
}

/// The column `PrimaryKeyCodec::decode_leftmost` decodes.
fn leftmost_primary_key(metadata: &RegionMetadata) -> Option<&ColumnMetadata> {
    match metadata.primary_key_encoding {
        // Sparse keys always start with the metric engine table id.
        PrimaryKeyEncoding::Sparse => metadata.column_by_name(DATA_SCHEMA_TABLE_ID_COLUMN_NAME),
        PrimaryKeyEncoding::Dense => metadata.primary_key_columns().next(),
    }
}

fn split_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            split_and(&binary.left, out);
            split_and(&binary.right, out);
        }
        _ => out.push(expr),
    }
}

/// Returns the literal of a non-null, same-type `column = literal` equality.
fn eq_literal<'a>(
    expr: &'a Expr,
    name: &str,
    data_type: &datatypes::arrow::datatypes::DataType,
) -> Option<&'a ScalarValue> {
    let Expr::BinaryExpr(binary) = expr else {
        return None;
    };
    if binary.op != Operator::Eq {
        return None;
    }
    let (column, value) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), Expr::Literal(value, _))
        | (Expr::Literal(value, _), Expr::Column(column)) => (column, value),
        _ => return None,
    };
    (column.name == name && !value.is_null() && &value.data_type() == data_type).then_some(value)
}

fn time_filter(expr: &Expr, name: &str, data_type: &datatypes::arrow::datatypes::DataType) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    if binary.op == Operator::And {
        return time_filter(&binary.left, name, data_type)
            && time_filter(&binary.right, name, data_type);
    }
    if !matches!(
        binary.op,
        Operator::Eq | Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq
    ) {
        return false;
    }
    let (column, value) = match (binary.left.as_ref(), binary.right.as_ref()) {
        (Expr::Column(column), Expr::Literal(value, _))
        | (Expr::Literal(value, _), Expr::Column(column)) => (column, value),
        _ => return false,
    };
    column.name == name && !value.is_null() && &value.data_type() == data_type
}

fn count_files(files: &[&FileHandle]) -> Option<usize> {
    files.iter().try_fold(0usize, |total, file| {
        let rows = usize::try_from(file.meta_ref().num_rows)
            .ok()
            .filter(|&n| n > 0)?;
        total.checked_add(rows)
    })
}

/// Sources for two independently consumable scans of the same read view.
#[derive(Clone)]
pub(crate) struct StatisticsSplitPlan {
    pub(crate) exact_files: Vec<FileHandle>,
    pub(crate) remaining_files: Vec<FileHandle>,
    pub(crate) exact_rows: usize,
    pub(crate) remaining_rows: Option<usize>,
}

/// Classifies individual sources instead of rejecting a mixed region wholesale.
pub(crate) fn statistics_split(
    input: &ScanInput,
    request: &ScanRequest,
) -> Option<StatisticsSplitPlan> {
    if input.compaction
        || input.series_row_selector.is_some()
        || input.sequence_range.is_some()
        || request.limit.is_some()
        || request.memtable_min_sequence.is_some()
        || request.memtable_max_sequence.is_some()
        || request.sst_min_sequence.is_some()
        || request.snapshot_on_scan
        || request.exact_sequence_range
        || request.skip_sst_files
    {
        return None;
    }
    #[cfg(feature = "enterprise")]
    if !input.extension_ranges.is_empty() {
        return None;
    }
    let range = input.time_range.as_ref()?;
    let count_filters = CountFilters::new(input.region_metadata(), &request.filters);

    // Check every temporally intersecting pair. Compaction outputs with disjoint
    // primary key ranges remain independent even when their time ranges overlap.
    let mut intervals = Vec::with_capacity(input.files.len());
    for (index, file) in input.files.iter().enumerate() {
        let (start, end) = file.time_range();
        if start > end {
            return None;
        }
        if range.intersects(&TimestampRange::new_inclusive(Some(start), Some(end))) {
            intervals.push((start, end, index));
        }
    }
    intervals.sort_unstable_by_key(|interval| interval.0);
    let mut overlaps = vec![false; input.files.len()];
    let mapper = input.primary_key_mapper();
    for (position, &(_, end, index)) in intervals.iter().enumerate() {
        for &(_, _, other_index) in intervals[position + 1..]
            .iter()
            .take_while(|interval| interval.0 <= end)
        {
            let file = &input.files[index];
            let other = &input.files[other_index];
            if file.raw_primary_key_range().is_none()
                || other.raw_primary_key_range().is_none()
                || files_overlap_inclusive(file, other, mapper)
            {
                overlaps[index] = true;
                overlaps[other_index] = true;
            }
        }
    }
    let memtable_stats: Vec<_> = input.memtables.iter().map(|m| m.stats()).collect();
    let mut exact_files = Vec::new();
    let mut remaining_files = Vec::new();
    let mut exact_rows = 0usize;
    for (index, file) in input.files.iter().enumerate() {
        let (start, end) = file.time_range();
        if !range.intersects(&TimestampRange::new_inclusive(Some(start), Some(end))) {
            continue;
        }
        let isolated_from_memory = memtable_stats.iter().all(|stats| match stats.time_range {
            Some((mem_start, mem_end)) => end < mem_start || mem_end < start,
            None => stats.num_rows == 0,
        });
        let eligible = count_filters
            .as_ref()
            .is_some_and(|filters| filters.covers(file))
            && range.contains(&start)
            && range.contains(&end)
            && file.num_rows() > 0
            && (input.region_partition_expr.is_none() || input.should_skip_region_partition(file))
            && (input.append_mode
                || (file.meta_ref().num_rows_is_logical
                    && !overlaps[index]
                    && isolated_from_memory));
        if eligible {
            exact_rows = exact_rows.checked_add(usize::try_from(file.meta_ref().num_rows).ok()?)?;
            exact_files.push(file.clone());
        } else {
            remaining_files.push(file.clone());
        }
    }
    if exact_files.is_empty() && remaining_files.is_empty() && input.memtables.is_empty() {
        return None;
    }
    let remaining_rows = remaining_files
        .iter()
        .try_fold(0usize, |rows, file| {
            let count = usize::try_from(file.meta_ref().num_rows)
                .ok()
                .filter(|rows| *rows > 0)?;
            rows.checked_add(count)
        })
        .and_then(|rows| {
            memtable_stats
                .iter()
                .try_fold(rows, |rows, stats| rows.checked_add(stats.num_rows))
        });
    Some(StatisticsSplitPlan {
        exact_files,
        remaining_files,
        exact_rows,
        remaining_rows,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use common_time::Timestamp;
    use common_time::timestamp::TimeUnit;
    use datafusion_expr::{col, lit};
    use datatypes::value::ValueRef;
    use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt};
    use store_api::storage::RegionId;
    use table::predicate::build_time_range_predicate;

    use super::*;
    use crate::read::flat_projection::FlatProjectionMapper;
    use crate::read::scan_region::PredicateGroup;
    use crate::sst::file::FileMeta;
    use crate::test_util::memtable_util::metadata_with_primary_key;
    use crate::test_util::new_noop_file_purger;
    use crate::test_util::scheduler_util::SchedulerEnv;

    fn file(start: i64, end: i64, rows: u64, logical: bool) -> FileHandle {
        FileHandle::new(
            FileMeta {
                num_rows: rows,
                num_rows_is_logical: logical,
                time_range: (
                    Timestamp::new_millisecond(start),
                    Timestamp::new_millisecond(end),
                ),
                ..Default::default()
            },
            new_noop_file_purger(),
        )
    }

    /// A certified file whose primary keys span `min..=max` over `(k0, k1)`.
    fn keyed_file(
        start: i64,
        end: i64,
        rows: u64,
        min: (&str, u32),
        max: (&str, u32),
    ) -> FileHandle {
        let codec = DensePrimaryKeyCodec::new(&metadata_with_primary_key(vec![0, 1], false));
        let encode = |(k0, k1): (&str, u32)| {
            Bytes::from(
                codec
                    .encode([ValueRef::String(k0), ValueRef::UInt32(k1)].into_iter())
                    .unwrap(),
            )
        };
        FileHandle::new(
            FileMeta {
                region_id: RegionId::new(123, 456),
                num_rows: rows,
                num_rows_is_logical: true,
                time_range: (
                    Timestamp::new_millisecond(start),
                    Timestamp::new_millisecond(end),
                ),
                primary_key_min: Some(encode(min)),
                primary_key_max: Some(encode(max)),
                ..Default::default()
            },
            new_noop_file_purger(),
        )
    }

    async fn input(files: Vec<FileHandle>, filters: &[Expr]) -> ScanInput {
        let env = SchedulerEnv::new().await;
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let mapper = FlatProjectionMapper::new(&metadata, [0, 2, 3]).unwrap();
        ScanInput::builder(env.access_layer.clone(), mapper)
            .with_files(files)
            .with_predicate(PredicateGroup::new(&metadata, filters).unwrap())
            .with_time_range(Some(build_time_range_predicate(
                "ts",
                TimeUnit::Millisecond,
                filters,
            )))
            .build()
    }

    #[tokio::test]
    async fn test_metadata_count_coverage_and_overlap() {
        let ts = |v| lit(ScalarValue::TimestampMillisecond(Some(v), None));
        for (filters, expected) in [
            (vec![], Some(5)),
            (vec![col("ts").lt(ts(20))], Some(3)),
            (vec![col("ts").gt_eq(ts(20))], Some(2)),
            (vec![col("ts").lt(ts(5))], None),
            (vec![col("ts").gt(ts(30))], Some(0)),
            (vec![col("ts").eq(ts(0)).or(col("ts").eq(ts(30)))], None),
            (vec![col("ts").not_eq(ts(5))], None),
            (vec![col("k0").eq(lit("x"))], None),
        ] {
            let input = input(vec![file(0, 10, 3, true), file(20, 30, 2, true)], &filters).await;
            let request = ScanRequest {
                filters,
                metadata_count_hint: true,
                ..Default::default()
            };
            // There are no physical files. Success proves that eligible manifest
            // counts do not open SSTs, even when their footer is not cached.
            assert_eq!(prepare(&input, &request).await, expected, "{request:?}");
        }
        for files in [
            vec![file(0, 10, 3, true), file(10, 20, 2, true)],
            vec![file(0, 10, 3, true), file(5, 8, 2, true)],
            vec![file(0, 10, 3, false)],
            vec![file(0, 10, 0, false)],
        ] {
            assert_eq!(
                prepare(&input(files, &[]).await, &ScanRequest::default()).await,
                None
            );
        }
    }

    #[tokio::test]
    async fn test_metadata_count_leftmost_key_filter() {
        let ts = |v| lit(ScalarValue::TimestampMillisecond(Some(v), None));
        let x = |start, end, rows| keyed_file(start, end, rows, ("x", 1), ("x", 9));
        let mixed = || keyed_file(20, 30, 2, ("x", 1), ("y", 1));
        let k0 = |v: &str| col("k0").eq(lit(v));
        for (files, filters, expected) in [
            // Both bounds prove the equality, so it holds for every row.
            (
                vec![x(0, 10, 3), x(20, 30, 2)],
                vec![col("ts").gt_eq(ts(0)).and(col("ts").lt(ts(40))), k0("x")],
                Some(5),
            ),
            (
                vec![x(0, 10, 3), x(20, 30, 2)],
                vec![lit("x").eq(col("k0"))],
                Some(5),
            ),
            // A file spanning several leftmost values may hold other rows.
            (vec![x(0, 10, 3), mixed()], vec![k0("x")], None),
            // A disproved equality is not used to exclude files.
            (vec![x(0, 10, 3)], vec![k0("y")], None),
            // Only the leftmost column is decodable from the bounds.
            (vec![x(0, 10, 3)], vec![col("k1").eq(lit(1u32))], None),
            (vec![x(0, 10, 3)], vec![col("k0").not_eq(lit("y"))], None),
            // Missing key statistics cannot prove anything.
            (vec![file(0, 10, 3, true)], vec![k0("x")], None),
        ] {
            let input = input(files, &filters).await;
            let request = ScanRequest {
                filters,
                metadata_count_hint: true,
                ..Default::default()
            };
            assert_eq!(prepare(&input, &request).await, expected, "{request:?}");
        }

        let filters = vec![k0("x")];
        let input = input(vec![x(0, 10, 3), mixed()], &filters).await;
        let request = ScanRequest {
            filters,
            ..Default::default()
        };
        let split = statistics_split(&input, &request).unwrap();
        assert_eq!((split.exact_rows, split.remaining_files.len()), (3, 1));
    }

    #[tokio::test]
    async fn test_metadata_count_fences_and_limits() {
        let input = input(vec![file(0, 10, 3, true)], &[]).await;
        for request in [
            ScanRequest {
                limit: Some(1),
                ..Default::default()
            },
            ScanRequest {
                memtable_max_sequence: Some(10),
                ..Default::default()
            },
            ScanRequest {
                memtable_min_sequence: Some(10),
                ..Default::default()
            },
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
            ScanRequest {
                sst_min_sequence: Some(10),
                ..Default::default()
            },
            ScanRequest {
                exact_sequence_range: true,
                ..Default::default()
            },
            ScanRequest {
                skip_sst_files: true,
                ..Default::default()
            },
        ] {
            assert_eq!(prepare(&input, &request).await, None);
        }
        let mut input = input;
        input.files = (0..=MAX_FILES)
            .map(|i| file(i as i64, i as i64, 1, true))
            .collect();
        assert_eq!(prepare(&input, &ScanRequest::default()).await, None);
        input.files.clear();
        assert_eq!(prepare(&input, &ScanRequest::default()).await, Some(0));
    }
    #[tokio::test]
    async fn test_metadata_count_unknown_manifest_count_falls_back() {
        let mut input = input(vec![file(0, 10, 0, true)], &[]).await;
        assert_eq!(prepare(&input, &ScanRequest::default()).await, None);
        input.append_mode = true;
        assert_eq!(prepare(&input, &ScanRequest::default()).await, None);
    }
    #[tokio::test]
    async fn test_metadata_count_statistics_split_sources() {
        for (files, exact_rows, remaining, estimate) in [
            (
                vec![file(0, 10, 3, true), file(20, 30, 2, false)],
                3,
                1,
                Some(2),
            ),
            (
                vec![
                    file(0, 100, 3, true),
                    file(5, 8, 2, false),
                    file(20, 30, 4, true),
                ],
                0,
                3,
                Some(9),
            ),
            (
                vec![file(0, 10, 3, true), file(20, 30, 0, false)],
                3,
                1,
                None,
            ),
        ] {
            let input = input(files, &[]).await;
            let split = statistics_split(&input, &ScanRequest::default()).unwrap();
            assert_eq!(split.exact_rows, exact_rows);
            assert_eq!(split.remaining_files.len(), remaining);
            assert_eq!(split.remaining_rows, estimate);
        }
    }
}
