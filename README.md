# 雨量站采录台

纯本地证据采集与业务决策工具。输入为虚构 fixture，运行中不访问网络。Python 3.11+ 标准库为运行依赖，pytest 为测试依赖。

## 快速开始

`python -m pip install . --no-build-isolation --no-deps` 在本地安装已预装构建工具的环境中构建此项目；运行期无第三方依赖。

`python -m rainstation demo` 自动初始化临时工作区、导入 fixture、计算决策并打印报告路径。`python -m rainstation --help` 查看手动导入、复核及报告命令。`python -m pytest -q` 运行自动化测试。

## 数据流和约束

配置文件的 SHA-256 冻结到 `meta`；`source` 存每个输入文件的哈希；`event` 存逐行规范化记录和事件身份；`issue` 保存解析或业务问题；`decision` 保存规则版本、业务结论和引用的事件 ID。导入采用单个 SQLite 事务；同文件哈希重入不重复写，同事件身份但不同内容进入冲突队列。业务规则读取事件时间而非文件顺序；未知对象或缺证据不能自动视为正常。决策重算保持源事件不可变。

错误码见 `issue.code`，报告同时输出 JSON 和 Markdown。`fixtures` 中是确定性的全虚构数据。首版只支持单机、单批；配置一旦冻结，变化需新建工作区。细分业务边界与未来方向由项目生成提示词约束，首版没有预置未来功能。

## 采录终端固件灰度升级与断线恢复

`rainstation/terminal_firmware.py` 是单台手持终端的本地安装/回退端点；`rainstation/rollout.py` 在其上提供跨终端灰度编排。本轮设备集合、单元与基线版本由冻结 case 摘要和事件表中匹配的设备脉冲来源摘要（SHA-256）共同确定，来源摘要不符或端点未知的终端不发请求，直接标记待人工处理。

每次安装或回退发出前，意图、稳定操作 ID（`<rollout>:<device>:<command>:1`）、预期 revision 与恢复阶段先写入 `firmware_intent` 流水。三类故障严格区分：未送达（`not_delivered`，现场不变、可重试）、明确拒绝（`rejected`）、实际提交但应答丢失（端点提交后经同一操作 ID 重放与 `state` 现场复核确认）。任何请求前后崩溃重启，都依据本地流水与现场 revision/版本恢复，同一 rollout 重跑幂等：不重复安装、不漏掉已提交设备、不跳过未知设备。回退只比较请求目标版本与 case 原版，现场当前版本仅通过 revision 校验；若其他维护员已改版导致 revision 不符，停止覆盖并标记 `rollback_revision_conflict` 待人工处理。

两份获授权合同：`fleet_atomic` 要求三台终端最终同版，任一单元拒绝即安全回退本轮已升级者；`cell_isolated` 容许 Ridge 与 Valley 不同版，但同一单元内必须同版，半升级单元经补偿后仍以 `half_upgraded_cell_recovered` 标注，绝不宣告完成。报告中的实版/实 revision 全部来自现场状态查询或端点返回的持久证据，计划版本与雨量报表不会被当作升级结果。

CLI：`firmware-register` 登记 case、`firmware-preflight` 核对来源摘要与现场实版、`firmware-rollout --contract {fleet_atomic,cell_isolated}` 执行或恢复升级（可重复运行）、`firmware-state` 查询单台现场实版、`firmware-report` 导出逐终端 JSON/Markdown 运维证据报告。编排状态保存在 `firmware_rollout`、`firmware_intent`、`firmware_rollout_device` 三张表中，对旧库通过 `CREATE TABLE IF NOT EXISTS` 幂等迁移。
