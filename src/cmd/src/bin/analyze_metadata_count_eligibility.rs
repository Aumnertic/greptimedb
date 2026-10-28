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

//! Offline, file-level evidence only; this does not prove query eligibility.

#![allow(clippy::print_stdout)]

use clap::Parser;
use common_time::Timestamp;
use mito2::config::MitoConfig;
use mito2::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use mito2::region::ManifestStats;
use mito2::sst::file::FileMeta;
use object_store::{ObjectStore, services::Fs};

#[derive(Parser)]
struct Args {
    /// Object store root of the stopped datanode.
    #[arg(long)]
    data_home: String,
    /// Region directory relative to the object store root, including data/metadata suffix.
    #[arg(long)]
    region_dir: String,
    /// Start of the 1/7/14 day query windows, inclusive, in Unix milliseconds.
    #[arg(long)]
    start_ms: i64,
}

fn overlap(a: &FileMeta, b: &FileMeta) -> bool {
    a.time_range.0 <= b.time_range.1 && b.time_range.0 <= a.time_range.1
}

fn report(files: &[&FileMeta], name: &str, bounds: Option<(Timestamp, Timestamp)>) -> (bool, bool) {
    let selected: Vec<_> = files
        .iter()
        .copied()
        .filter(|f| {
            bounds.is_none_or(|(start, end)| f.time_range.1 >= start && f.time_range.0 < end)
        })
        .collect();
    let full_cover = |f: &FileMeta| {
        bounds.is_none_or(|(start, end)| f.time_range.0 >= start && f.time_range.1 < end)
    };
    let overlaps = selected
        .iter()
        .enumerate()
        .filter(|(i, f)| {
            selected
                .iter()
                .enumerate()
                .any(|(j, g)| *i != j && overlap(f, g))
        })
        .count();
    let certified = selected
        .iter()
        .filter(|f| f.num_rows_is_logical && f.num_rows > 0)
        .count();
    let isolated = selected
        .iter()
        .enumerate()
        .filter(|(i, f)| {
            f.num_rows_is_logical
                && f.num_rows > 0
                && full_cover(f)
                && !selected
                    .iter()
                    .enumerate()
                    .any(|(j, g)| *i != j && overlap(f, g))
        })
        .count();
    let rows: u128 = selected.iter().map(|f| u128::from(f.num_rows)).sum();
    let certified_rows: u128 = selected
        .iter()
        .filter(|f| f.num_rows_is_logical && f.num_rows > 0 && full_cover(f))
        .map(|f| u128::from(f.num_rows))
        .sum();
    let file_conditions = files.len() <= 64
        && files.iter().all(|f| f.time_range.0 <= f.time_range.1)
        && certified == selected.len()
        && overlaps == 0
        && selected.iter().all(|f| full_cover(f));
    println!(
        "\n{name}: files={} certified={} overlapping_files={} isolated_certified={} non_append_file_conditions={}",
        selected.len(),
        certified,
        overlaps,
        isolated,
        file_conditions
    );
    if selected.iter().any(|f| f.num_rows == 0) || rows == 0 {
        println!("  certified physical row share: unknown (missing or zero total row counts)");
    } else {
        println!(
            "  certified fully covered physical row share: {:.1}%",
            certified_rows as f64 * 100.0 / rows as f64
        );
    }
    (file_conditions, !file_conditions && isolated > 0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let store = ObjectStore::new(Fs::default().root(&args.data_home))?;
    let options = RegionManifestOptions::new(&MitoConfig::default(), &args.region_dir, &store);
    let stats = ManifestStats::default();
    let manager = RegionManifestManager::open(options, &stats)
        .await?
        .ok_or("region manifest not found")?;
    let manifest = manager.manifest();
    let files: Vec<_> = manifest.files.values().collect();
    println!(
        "Region {}: {} live SST files, append_mode={:?}",
        manifest.metadata.region_id,
        files.len(),
        manifest.append_mode
    );
    println!(
        "File conditions alone do not establish SQL eligibility: memtables, predicates, sequence fences, partition filtering, scan mode and logical-region wrapping are not checked."
    );
    let now = Timestamp::current_millis().value();
    let mut buckets = [0usize; 5];
    for file in &files {
        let age = file
            .time_range
            .1
            .convert_to(common_time::timestamp::TimeUnit::Millisecond)
            .and_then(|ts| now.checked_sub(ts.value()));
        let index = match age {
            Some(age) if (0..86_400_000).contains(&age) => 0,
            Some(age) if (0..7 * 86_400_000).contains(&age) => 1,
            Some(age) if (0..14 * 86_400_000).contains(&age) => 2,
            Some(age) if age >= 0 => 3,
            _ => 4,
        };
        buckets[index] += 1;
    }
    println!("File age by maximum timestamp (relative to wall clock):");
    for (label, count) in ["0-1d", "1-7d", "7-14d", "14d+", "future/unknown"]
        .iter()
        .zip(buckets)
    {
        println!("  {label}: {count}");
    }
    let mut outcomes = vec![report(&files, "all", None)];
    for days in [1_i64, 7, 14] {
        let end = args
            .start_ms
            .checked_add(days * 86_400_000)
            .ok_or("window overflow")?;
        outcomes.push(report(
            &files,
            &format!("{days}d"),
            Some((
                Timestamp::new_millisecond(args.start_ms),
                Timestamp::new_millisecond(end),
            )),
        ));
    }
    println!(
        "\nFile-level summary over these four sample windows: {} full candidates, {} partial candidates",
        outcomes.iter().filter(|(full, _)| *full).count(),
        outcomes.iter().filter(|(_, partial)| *partial).count()
    );
    println!(
        "These sample windows do not represent query frequency or prove that either optimization applies."
    );
    Ok(())
}
