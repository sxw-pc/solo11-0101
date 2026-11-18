# 雨量站采录台设计

站点契约声明时区偏移、脉冲系数生效时间、最大时钟漂移、物理小时强度与可选相邻站对照。输入脉冲、校时锚点、维护窗口和设备心跳均保留来源文件、行号、事件身份和哈希。

数据流：站点契约 → 原始事件 → 设备序号缺口与锚点核对 → 时间修正 → 维护窗口排除 → 小时聚合与邻站诊断 → 完整性判定 → JSON/Markdown 水文报告。

脉冲用最近的有效校时锚点修正为 UTC；无锚点标记 `clock_anchor_missing`。锚点偏移差超过阈值为 `clock_drift`，不可假装可信。维护窗口采用 `[start,end)`；窗口内测试脉冲不计自然降水。小时桶在 UTC 半开区间聚合，再按站点时区标记。系数按生效时间选择，缺失为 `coefficient_missing`；物理强度超限为 `physical_spike`，邻站差异为 `neighbor_disagreement`。设备断号、心跳缺失或间隔过大分别标记，形成 `trusted`、`incomplete`、`review` 结论。

导入为事务，重复文件不重写；身份碰撞留 `identity_conflict`。每次重算只更新业务问题和决策，源事件不可变。当前不做实时遥测、洪峰预警或区域面雨量估算。

## 低内存流式采录与来源快照合同

导入按有界块读取（默认 64 KiB，`--chunk-size` 可调），用 `codecs` 增量解码器处理跨块 UTF-8 BOM 与多字节字符，逐行解析；Python 进程不持有完整 bytes、完整文本或全量行数组，峰值内存随块大小而非文件总长增长。行边界与 `decode('utf-8-sig').splitlines()` 对齐：`\r\n`、孤立 `\r`、`\n` 均为行结束，空行仍占用物理行号，坏行进 `parse_error` 隔离且 detail 保留原文行。

每块的字节同时驱动三件事，且来自同一个已打开的字节流：增量 SHA-256、`staging_source.raw` 的增量 BLOB 写入（`zeroblob` 占位 + `blobopen`）、逐行事件暂存。因此正式 `source.raw` 与记录的 SHA-256 逐字节一致，`event.payload/source_id/line_no` 可按行号回指同一原文。

暂存表 `snapshot/staging_source/staging_event/staging_issue` 与正式表共享一个 `snapshot_id`。读取或暂存阶段中断后，重入同一快照身份：已完整暂存（哈希与长度自洽）则直接原子晋升，否则整组清理残留后重做；晋升在单个事务内完成，失败整体回滚，不留半份正式 source/event/issue，也不产生重复事件。同一快照重入按源哈希幂等，同事件身份异内容继续进入 `identity_conflict` 隔离。

同一逻辑入站路径在读取中途被外部原子替换（inode 变化）时，两种授权合同都保证内部一致但采录结果不同：

- `freeze_on_open`（默认）：固定起始打开的版本读完，路径替换不影响本次证据，正式 raw/hash/event 全部属于原版。
- `verify_then_retry_latest`：读完发现路径已换版时废弃前一暂存，对最终稳定新版本重试；反复换版超过上限则抛 `SnapshotUnstableError`，不提交任何正式来源。

两合同的差异落在真实入库的 raw、SHA-256 与事件哈希上，而非报告文字。
