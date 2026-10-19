# Mito 混合 COUNT demo

用真实 Mito 引擎写入并 flush 出四个 SST，保留一份未 flush 的 memtable。
同一份 Version、无并发写入下，对比全扫描计数与“精确统计 + fallback 扫描”。

这是存储层实验，通过 Rust 测试运行；尚未接入 SQL 优化器或正式 CBO。
`Inexact` 和 `Absent` 都实际扫描，最终 COUNT 始终必须精确。

## 运行

在仓库根目录执行，首次需要编译 Mito：

```bash
cargo nextest run -p mito2 --lib -E 'test(hybrid_count_demo)' --no-capture
```

已在 `dev` 的独立 worktree 编译运行通过：1 个测试中包含 6 组对照。
可直接重新运行远端已配置好的脚本，本机不编译：

```bash
ssh dev 'bash /home/zed/projects/greptimedb-ospp-runs/20261019-mito-hybrid-count/run.sh'
```

本次运行的实际报告保存在 [`demo-output.json`](demo-output.json)。

代码位于
[`hybrid_count_demo.rs`](../../src/mito2/src/read/scan_region/hybrid_count_demo.rs)。
依次阅读 `CountQuery`、`build_count_plan()`、`scan_count()` 和 `run_demo()`。
入口只通过 `#[cfg(test)]` 编译，不改变生产查询行为。

## 数据与预期结果

数据使用毫秒时间戳，下面为便于阅读换算成秒：

| 来源 | 时间戳 | 行数 | 查询 `0 <= ts < 15s` 时 |
| --- | --- | ---: | --- |
| SST A | 0..9s | 10 | 精确元数据直接贡献 10 |
| SST B | 10..19s | 10 | 部分重叠，扫描后贡献 5 |
| SST C | 2..4s | 3 | 模拟历史元数据缺失行数，扫描贡献 3 |
| SST D | 30..34s | 5 | 不相交，跳过 |
| memtable | 4..6s | 3 | 保守扫描，贡献 3 |

append-only 下：`10 + (5 + 3 + 3) = 21`，等于全扫描结果。
只打开 SST B、C，不打开直接计数的 A 或被裁剪的 D。

C、memtable 刻意重复 A 的主键和时间戳。append-only 下它们是应保留的额外行。
普通 update/dedup 模式下整个计划回退为扫描，结果为 15，不能相加物理行数。

每种模式运行三个场景：混合路径、全部文件行数未知、空时间范围，共六次对照。
输出 JSON 包含每个来源的 `action`、`precision`、`reason`，以及统计贡献、
实际扫描行数和 baseline。这里的 JSON 是 demo 报告，不是正式 EXPLAIN JSON。

## 验证的内容

- 两条路径固定同一个 Version，fixture 在取快照后不再写入或 flush。
- 所有候选 SST 恰好进入元数据分支或扫描分支之一，两个集合不相交。
- 用 object-store mock layer 记录实际读取路径，断言元数据分支从未打开 SST，
  fallback 文件确实被打开；扫描关闭缓存，避免缓存命中掩盖验证。
- baseline、混合结果和预设答案三方一致。
- 历史缺失统计通过删除真实 FileMeta 序列化结果中的 `num_rows` 再反序列化模拟。
  只影响 demo 的统计视图，原 SST、manifest 和 fallback reader 的文件句柄保持原样。

第一版只接收生成的时间范围谓词。append-only、完整时间覆盖、有行数、无额外
可见性过滤时才直接计数。memtable 一律扫描。版本引用本身不能冻结可变 memtable；
本 demo 用无并发写入的 fixture 保证稳定，不能据此声明生产环境的快照计数已实现。

这个实验验证 Exact 判定和 fallback。项目验收仍需另外完成 query-local 统计桥接、
简单谓词估算、local JoinSelection 和正式 scan-level EXPLAIN JSON。
