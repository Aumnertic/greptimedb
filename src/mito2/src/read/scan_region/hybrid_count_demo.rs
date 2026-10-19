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

//! Runnable storage-level demo: exact metadata counts + ordinary Mito scans.
//! All helpers are test-only. There are no writes after the snapshot is pinned.

use std::collections::{BTreeSet, HashSet};
use std::sync::{Arc, Mutex};

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use object_store::layers::mock::MockLayerBuilder;
use serde_json::{Value, json};
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use super::{ScanInput, ScanRegion, time_range_covers_file};
use crate::cache::CacheStrategy;
use crate::config::MitoConfig;
use crate::read::seq_scan::SeqScan;
use crate::sst::file::{FileHandle, FileMeta, RegionFileId};
use crate::test_util::{self, CreateRequestBuilder, TestEnv};

type ReadPaths = Arc<Mutex<BTreeSet<String>>>;

/// The demo deliberately accepts only a half-open range on the time index.
/// An arbitrary predicate must never be inferred to hold from time bounds alone.
#[derive(Clone, Copy)]
struct CountQuery {
    start_ms: i64,
    end_ms: i64,
}

impl CountQuery {
    fn request(self) -> ScanRequest {
        let timestamp = |value| lit(ScalarValue::TimestampMillisecond(Some(value), None));
        ScanRequest {
            projection: Some(vec![2]),
            filters: vec![
                col("ts").gt_eq(timestamp(self.start_ms)),
                col("ts").lt(timestamp(self.end_ms)),
            ],
            ..Default::default()
        }
    }
}

struct CountPlan {
    metadata_rows: u64,
    metadata_files: HashSet<RegionFileId>,
    fallback: ScanInput,
    sources: Vec<Value>,
}

/// Splits each selected SST exactly once; memtables always stay in the scan.
/// Missing historical counts are simulated by deserializing metadata without
/// `num_rows`. Actual files and the engine's manifest are left unchanged.
fn build_count_plan(mut input: ScanInput, missing_counts: &HashSet<RegionFileId>) -> CountPlan {
    let can_use_metadata = input.append_mode
        && input.region_partition_expr.is_none()
        && input.sequence_range.is_none()
        && input.snapshot_sequence.is_none()
        && input.series_row_selector.is_none()
        && !input.compaction;
    let mut metadata_rows = 0_u64;
    let mut metadata_files = HashSet::new();
    let mut fallback_files = Vec::new();
    let mut sources = Vec::new();

    input
        .files
        .sort_by_key(|file| (file.time_range().0.value(), file.file_id().to_string()));
    for file in std::mem::take(&mut input.files) {
        let mut meta = file.meta_ref().clone();
        if missing_counts.contains(&file.file_id()) {
            let mut historical = serde_json::to_value(&meta).unwrap();
            historical.as_object_mut().unwrap().remove("num_rows");
            meta = serde_json::from_value::<FileMeta>(historical).unwrap();
            assert_eq!(meta.num_rows, 0);
        }
        let covered = time_range_covers_file(input.time_range.as_ref(), &file);
        let (action, precision, reason) = if !can_use_metadata {
            ("scan", "Absent", "visible_rows_not_proven")
        } else if meta.num_rows == 0 {
            ("scan", "Absent", "missing_row_count")
        } else if !covered {
            ("scan", "Inexact", "partial_time_overlap")
        } else if let Some(total) = metadata_rows.checked_add(meta.num_rows) {
            metadata_rows = total;
            assert!(metadata_files.insert(file.file_id()));
            ("metadata", "Exact", "all_file_rows_match")
        } else {
            ("scan", "Absent", "count_overflow")
        };
        sources.push(json!({
            "source": "sst",
            "file_id": file.file_id().to_string(),
            "time_range_ms": [meta.time_range.0.value(), meta.time_range.1.value()],
            "metadata_rows": (meta.num_rows != 0).then_some(meta.num_rows),
            "action": action,
            "precision": precision,
            "reason": reason,
        }));
        if action == "scan" {
            // Keep the real file handle: missing stats affect the decision,
            // while the fallback reader still opens the original physical SST.
            fallback_files.push(file);
        }
    }
    for memtable in &input.memtables {
        sources.push(json!({
            "source": "memtable_range",
            "physical_rows": memtable.stats().num_rows(),
            "action": "scan",
            "precision": "Absent",
            "reason": "memtable_fallback",
        }));
    }
    input.files = fallback_files;
    CountPlan {
        metadata_rows,
        metadata_files,
        fallback: input,
        sources,
    }
}

async fn scan_count(input: ScanInput) -> u64 {
    let stream = SeqScan::new(input).build_stream().unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    batches
        .iter()
        .try_fold(0_u64, |rows, batch| {
            rows.checked_add(u64::try_from(batch.num_rows()).unwrap())
        })
        .unwrap()
}

#[tokio::test]
async fn test_hybrid_count_demo() {
    run_demo(true).await;
    run_demo(false).await;
}

async fn run_demo(append_mode: bool) {
    let read_paths: ReadPaths = Arc::default();
    let observed = read_paths.clone();
    let mock_layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _args, reader| {
            observed.lock().unwrap().insert(path.to_string());
            reader
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::with_prefix("hybrid_count_demo")
        .await
        .with_mock_layer(mock_layer);
    let engine = env
        .create_engine(MitoConfig {
            num_workers: 2,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(1234, 1);
    let create = CreateRequestBuilder::new()
        .insert_option("append_mode", if append_mode { "true" } else { "false" })
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .insert_option("compaction.twcs.active_window.trigger_file_num", "1000")
        .insert_option("compaction.twcs.inactive_window.trigger_file_num", "1000")
        .build();
    let schema = test_util::rows_schema(&create);
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();

    // A: fully covered; B: partially covered; C: fully covered but missing its
    // row count in the demo's stats view; D: outside the query time range.
    // C repeats rows from A, deliberately exercising append vs dedup semantics.
    for (start, end) in [(0, 10), (10, 20), (2, 5), (30, 35)] {
        test_util::put_rows(
            &engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows: test_util::build_rows(start, end),
            },
        )
        .await;
        test_util::flush_region(&engine, region_id, None).await;
    }
    test_util::put_rows(
        &engine,
        region_id,
        Rows {
            schema,
            rows: test_util::build_rows(4, 7),
        },
    )
    .await;

    // Hold one Version for both paths. The fixture performs no further writes,
    // flushes, or compactions; pinning Version alone would not freeze a mutable
    // memtable under concurrent writes in a production implementation.
    let region = engine.find_region(region_id).unwrap();
    let version = region.version_control.current().version;
    let all_files: Vec<_> = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .cloned()
        .collect();
    assert_eq!(
        all_files.len(),
        4,
        "fixture requires four independent flushes"
    );
    let old_file = all_files
        .iter()
        .find(|file| file.time_range().0 == Timestamp::new_millisecond(2000))
        .unwrap()
        .file_id();

    for (case, query, missing_counts) in [
        (
            "mixed",
            CountQuery {
                start_ms: 0,
                end_ms: 15_000,
            },
            HashSet::from([old_file]),
        ),
        (
            "all_unknown",
            CountQuery {
                start_ms: 0,
                end_ms: 15_000,
            },
            all_files.iter().map(FileHandle::file_id).collect(),
        ),
        (
            "empty",
            CountQuery {
                start_ms: 40_000,
                end_ms: 50_000,
            },
            HashSet::new(),
        ),
    ] {
        let build_scan = || {
            ScanRegion::new(
                version.clone(),
                region.access_layer.clone(),
                query.request(),
                CacheStrategy::Disabled,
            )
        };
        read_paths.lock().unwrap().clear();
        let baseline = scan_count(build_scan().scan_input().await.unwrap()).await;
        let baseline_paths = read_paths.lock().unwrap().clone();

        read_paths.lock().unwrap().clear();
        let plan = build_count_plan(build_scan().scan_input().await.unwrap(), &missing_counts);
        let fallback_files: HashSet<_> = plan
            .fallback
            .files
            .iter()
            .map(FileHandle::file_id)
            .collect();
        let selected_files: HashSet<_> = all_files
            .iter()
            .filter(|file| {
                super::file_in_range(
                    file,
                    &super::build_time_range_predicate(
                        "ts",
                        common_time::timestamp::TimeUnit::Millisecond,
                        &query.request().filters,
                    ),
                )
            })
            .map(FileHandle::file_id)
            .collect();
        assert!(plan.metadata_files.is_disjoint(&fallback_files));
        assert_eq!(
            plan.metadata_files
                .union(&fallback_files)
                .copied()
                .collect::<HashSet<_>>(),
            selected_files
        );

        let scanned_rows = scan_count(plan.fallback).await;
        let result = plan.metadata_rows.checked_add(scanned_rows).unwrap();
        assert_eq!(result, baseline);
        let hybrid_paths = read_paths.lock().unwrap().clone();
        for file in &all_files {
            let id = file.file_id().file_id().to_string();
            let was_read = |paths: &BTreeSet<String>| paths.iter().any(|path| path.contains(&id));
            if plan.metadata_files.contains(&file.file_id()) {
                assert!(
                    was_read(&baseline_paths),
                    "baseline must read metadata-counted SST"
                );
                assert!(
                    !was_read(&hybrid_paths),
                    "metadata-counted SST must not be opened"
                );
            } else if fallback_files.contains(&file.file_id()) {
                assert!(
                    was_read(&hybrid_paths),
                    "fallback SST must actually be opened"
                );
            } else {
                assert!(
                    !was_read(&hybrid_paths),
                    "time-pruned SST must not be opened"
                );
            }
        }
        if case == "mixed" && append_mode {
            assert_eq!((plan.metadata_rows, scanned_rows, result), (10, 11, 21));
            assert_eq!((plan.metadata_files.len(), fallback_files.len()), (1, 2));
        } else {
            assert_eq!(plan.metadata_rows, 0);
        }
        assert_eq!(
            result,
            if case == "empty" {
                0
            } else if append_mode {
                21
            } else {
                15
            }
        );
        println!("{}", serde_json::to_string_pretty(&json!({
            "case": case,
            "append_mode": append_mode,
            "query": format!("COUNT(*) WHERE ts >= {} AND ts < {} (milliseconds)", query.start_ms, query.end_ms),
            "sources": plan.sources,
            "metadata_files": plan.metadata_files.len(),
            "scanned_files": fallback_files.len(),
            "time_pruned_files": all_files.len() - plan.metadata_files.len() - fallback_files.len(),
            "metadata_rows": plan.metadata_rows,
            "scanned_rows": scanned_rows,
            "hybrid_count": result,
            "baseline_count": baseline,
            "verified_equal": true,
            "metadata_files_not_opened": true,
        })).unwrap());
    }
    drop(all_files);
    drop(version);
    drop(region);
    engine.stop().await.unwrap();
}
