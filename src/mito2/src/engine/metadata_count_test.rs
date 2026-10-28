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

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionContext;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use datatypes::arrow::datatypes::SchemaRef;
use object_store::layers::mock::MockLayerBuilder;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionDeleteRequest, RegionPutRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use table::table::scan::{PartialCountRule, statistics_scan_plan};

use crate::config::MitoConfig;
use crate::engine::compaction_test::{compact, delete_and_flush, put_and_flush};
use crate::test_util::{self, CreateRequestBuilder, TestEnv};

/// Exercises DataFusion's normal SQL physical optimizer over a real Mito scan.
/// GreptimeDB's SQL-to-count hint is covered separately by ScanHintRule tests.
#[derive(Debug)]
struct CountTable {
    plan: Arc<dyn ExecutionPlan>,
}

#[async_trait]
impl TableProvider for CountTable {
    fn schema(&self) -> SchemaRef {
        self.plan.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        if let Some(projection) = projection {
            let schema = self.plan.schema();
            let exprs: Vec<(Arc<dyn datafusion::physical_plan::PhysicalExpr>, String)> = projection
                .iter()
                .map(|index| {
                    let name = schema.field(*index).name();
                    (Arc::new(Column::new(name, *index)) as _, name.clone())
                })
                .collect();
            Ok(Arc::new(ProjectionExec::try_new(exprs, self.plan.clone())?))
        } else {
            Ok(self.plan.clone())
        }
    }
}

fn count_context() -> SessionContext {
    let mut optimizer = datafusion::physical_optimizer::optimizer::PhysicalOptimizer::new();
    optimizer
        .rules
        .insert(1, Arc::new(PartialCountRule::default()));
    let state = datafusion::execution::session_state::SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rules(optimizer.rules)
        .build();
    SessionContext::new_with_state(state)
}

#[tokio::test]
async fn test_metadata_count_compaction_sql_and_fallback() {
    let reads = Arc::new(AtomicUsize::new(0));
    let observed = reads.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner| {
            if path.ends_with(".parquet") {
                observed.fetch_add(1, Ordering::Relaxed);
            }
            inner
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(layer);
    let config = MitoConfig {
        experimental_metadata_count: true,
        min_compaction_interval: Duration::from_secs(3600),
        schedule_compaction_after_edit: false,
        ..Default::default()
    };
    let engine = env.create_engine(config.clone()).await;
    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "count_test",
            "greptime",
            "public",
            None,
            env.get_kv_backend(),
        )
        .await;
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .build();
    let schema = test_util::rows_schema(&create);
    let table_dir = create.table_dir.clone();
    let options = create.options.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    for range in [0..5, 2..7, 0..3] {
        put_and_flush(&engine, region_id, &schema, range).await;
    }
    delete_and_flush(&engine, region_id, &schema, 1..2).await;
    let request = ScanRequest {
        metadata_count_hint: true,
        ..Default::default()
    };
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(
        scanner.properties().exact_num_rows(),
        None,
        "flush does not certify logical rows"
    );
    compact(&engine, region_id).await;
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let files = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1);
    assert!(files[0].meta_ref().num_rows_is_logical);
    assert_eq!(files[0].meta_ref().num_rows, 6);
    drop(version);
    drop(region);

    // Verify the capability survives manifest persistence and reopening.
    test_util::reopen_region(&engine, region_id, table_dir, true, options).await;
    reads.store(0, Ordering::Relaxed);
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(scanner.properties().exact_num_rows(), Some(6));
    let scan = statistics_scan_plan(scanner, request.clone(), None, false).unwrap();
    let ctx = count_context();
    ctx.register_table("t", Arc::new(CountTable { plan: scan }))
        .unwrap();
    let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
    let plan = df.clone().create_physical_plan().await.unwrap();
    let text = displayable(plan.as_ref()).indent(true).to_string();
    assert!(!text.contains("RegionScanExec"), "{text}");
    let batches = df.collect().await.unwrap();
    assert_eq!(
        ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
        ScalarValue::Int64(Some(6))
    );
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "optimized COUNT must not open any SST"
    );

    let ts = |v| lit(ScalarValue::TimestampMillisecond(Some(v), None));
    for (filters, expected) in [
        (
            vec![col("ts").gt_eq(ts(0)).and(col("ts").lt(ts(7000)))],
            Some(6),
        ),
        (vec![col("ts").lt(ts(3000))], None),
        (vec![col("ts").gt(ts(7000))], Some(0)),
    ] {
        let scanner = engine
            .handle_query(
                region_id,
                ScanRequest {
                    filters,
                    ..request.clone()
                },
            )
            .await
            .unwrap();
        assert_eq!(scanner.properties().exact_num_rows(), expected);
    }

    // A new deletion in memory invalidates the complete count; ordinary scan
    // still resolves the tombstone against the compacted file.
    engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest {
                rows: api::v1::Rows {
                    schema: schema.clone(),
                    rows: test_util::build_rows_for_key("a", 2, 3, 0),
                },
                hint: None,
                partition_expr_version: None,
            }),
        )
        .await
        .unwrap();
    let scanner = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
    assert_eq!(scanner.properties().exact_num_rows(), None);
    let baseline = common_recordbatch::RecordBatches::try_collect(
        engine
            .scan_to_stream(region_id, ScanRequest::default())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        baseline.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        5
    );
    // Ordinary writes retain their original behavior.
    engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest {
                rows: api::v1::Rows {
                    schema,
                    rows: test_util::build_rows_for_key("a", 2, 3, 0),
                },
                hint: None,
                partition_expr_version: None,
                skip_wal: false,
            }),
        )
        .await
        .unwrap();
    engine.stop().await.unwrap();
}

#[tokio::test]
async fn test_metadata_count_fourteen_day_workload() {
    use api::v1::value::ValueData;
    use store_api::region_request::RegionFlushRequest;

    const DAY_SECONDS: usize = 86_400;
    let reads = Arc::new(Mutex::new(Vec::<String>::new()));
    let observed = reads.clone();
    let layer = MockLayerBuilder::default()
        .reader_factory(Arc::new(move |path, _, inner| {
            if path.ends_with(".parquet") {
                observed.lock().unwrap().push(path.to_string());
            }
            inner
        }))
        .build()
        .unwrap();
    let mut env = TestEnv::new().await.with_mock_layer(layer);
    let engine = env
        .create_engine(MitoConfig {
            experimental_metadata_count: true,
            min_compaction_interval: Duration::from_secs(3600),
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;
    let region_id = RegionId::new(2, 1);
    let create = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "7d")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .build();
    let schema = test_util::rows_schema(&create);
    engine
        .handle_request(region_id, RegionRequest::Create(create))
        .await
        .unwrap();
    for day in 0..14 {
        let mut rows = test_util::build_rows_for_key("a", 0, 1000, 0);
        for (minute, row) in rows.iter_mut().enumerate() {
            row.values[2].value_data = Some(ValueData::TimestampMillisecondValue(
                ((day * DAY_SECONDS + minute * 60) * 1000) as i64,
            ));
        }
        test_util::put_rows(
            &engine,
            region_id,
            api::v1::Rows {
                schema: schema.clone(),
                rows,
            },
        )
        .await;
        engine
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest::default()),
            )
            .await
            .unwrap();
        if day == 6 {
            compact(&engine, region_id).await;
        }
    }

    let historical_files: Vec<_> = engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .filter(|file| file.meta_ref().num_rows_is_logical)
        .map(|file| file.meta_ref().file_id.to_string())
        .collect();
    assert!(!historical_files.is_empty());

    let ts = |day: i64| {
        lit(ScalarValue::TimestampMillisecond(
            Some(day * 86_400_000),
            None,
        ))
    };
    for (filters, expected_count, optimized) in [
        (vec![], 14_000, false),
        (vec![col("ts").lt(ts(7))], 7_000, true),
        (vec![col("ts").gt_eq(ts(7))], 7_000, false),
        (
            vec![col("ts").gt_eq(ts(5)).and(col("ts").lt(ts(10)))],
            5_000,
            false,
        ),
    ] {
        reads.lock().unwrap().clear();
        let request = ScanRequest {
            metadata_count_hint: true,
            filters,
            ..Default::default()
        };
        let scanner = engine
            .handle_query(region_id, request.clone())
            .await
            .unwrap();
        assert_eq!(
            scanner.properties().exact_num_rows().is_some(),
            optimized,
            "{request:?}"
        );
        let scan = statistics_scan_plan(scanner, request, None, false).unwrap();
        if expected_count == 14_000 {
            use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
            use datafusion_common::stats::Precision;
            let children = scan.children();
            assert_eq!(children.len(), 2);
            let stats = StatisticsContext::new()
                .compute(children[0].as_ref(), &StatisticsArgs::new())
                .unwrap();
            assert_eq!(stats.num_rows, Precision::Exact(7000));
            let stats = StatisticsContext::new()
                .compute(children[1].as_ref(), &StatisticsArgs::new())
                .unwrap();
            assert_eq!(stats.num_rows, Precision::Inexact(7000));
            let stats = StatisticsContext::new()
                .compute(scan.as_ref(), &StatisticsArgs::new())
                .unwrap();
            assert_eq!(stats.num_rows, Precision::Inexact(14_000));
        }
        let ctx = count_context();
        ctx.register_table("t", Arc::new(CountTable { plan: scan }))
            .unwrap();
        let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert_eq!(!text.contains("SeqScan"), optimized, "{text}");
        if expected_count == 14_000 {
            assert!(text.contains("InexactRegionScanExec"), "{text}");
            assert!(
                text.contains("7000"),
                "exact COUNT state must survive: {text}"
            );
            assert!(
                !text.contains("ExactRegionScanExec"),
                "exact branch must be folded: {text}"
            );
        }
        let batches = df.collect().await.unwrap();
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
            ScalarValue::Int64(Some(expected_count))
        );
        if optimized {
            assert!(
                reads.lock().unwrap().is_empty(),
                "metadata-only COUNT opened an SST"
            );
        }
        if expected_count == 14_000 {
            let paths = reads.lock().unwrap();
            assert!(
                !paths.is_empty(),
                "remaining files must actually be scanned"
            );
            assert!(
                paths
                    .iter()
                    .all(|path| historical_files.iter().all(|id| !path.contains(id))),
                "exact files were read: {paths:?}"
            );
            drop(paths);
            let raw = ctx
                .sql("SELECT * FROM t")
                .await
                .unwrap()
                .collect()
                .await
                .unwrap();
            assert_eq!(
                raw.iter().map(|batch| batch.num_rows()).sum::<usize>(),
                14_000
            );
        }
    }
    for (timestamp, delete, expected, retains_exact) in [
        (15 * 86_400_000, false, 14_001, true),
        (86_400_000, true, 14_000, false),
    ] {
        let mut rows = test_util::build_rows_for_key("a", 0, 1, 0);
        rows[0].values[2].value_data = Some(ValueData::TimestampMillisecondValue(timestamp));
        let rows = api::v1::Rows {
            schema: schema.clone(),
            rows,
        };
        let request = if delete {
            RegionRequest::Delete(RegionDeleteRequest {
                rows,
                hint: None,
                partition_expr_version: None,
            })
        } else {
            RegionRequest::Put(RegionPutRequest {
                rows,
                hint: None,
                partition_expr_version: None,
                skip_wal: false,
            })
        };
        engine.handle_request(region_id, request).await.unwrap();
        let request = ScanRequest {
            metadata_count_hint: true,
            ..Default::default()
        };
        let scanner = engine
            .handle_query(region_id, request.clone())
            .await
            .unwrap();
        let plan = statistics_scan_plan(scanner, request, None, false).unwrap();
        let ctx = count_context();
        ctx.register_table("t", Arc::new(CountTable { plan }))
            .unwrap();
        let df = ctx.sql("SELECT count(*) FROM t").await.unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let text = displayable(plan.as_ref()).indent(true).to_string();
        assert_eq!(text.contains("7000 as"), retains_exact, "{text}");
        let batches = df.collect().await.unwrap();
        assert_eq!(
            ScalarValue::try_from_array(batches[0].column(0), 0).unwrap(),
            ScalarValue::Int64(Some(expected))
        );
    }
    engine.stop().await.unwrap();
}
