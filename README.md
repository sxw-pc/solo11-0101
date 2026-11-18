# 雨量站采录台

纯本地证据采集与业务决策工具。输入为虚构 fixture，运行中不访问网络。Python 3.11+ 标准库为运行依赖，pytest 为测试依赖。

## 快速开始

`python -m pip install . --no-build-isolation --no-deps` 在本地安装已预装构建工具的环境中构建此项目；运行期无第三方依赖。

`python -m rainstation demo` 自动初始化临时工作区、导入 fixture、计算决策并打印报告路径。`python -m rainstation --help` 查看手动导入、复核及报告命令。`python -m pytest -q` 运行自动化测试。

## 数据流和约束

配置文件的 SHA-256 冻结到 `meta`；`source` 存每个输入文件的哈希；`event` 存逐行规范化记录和事件身份；`issue` 保存解析或业务问题；`decision` 保存规则版本、业务结论和引用的事件 ID。导入采用单个 SQLite 事务；同文件哈希重入不重复写，同事件身份但不同内容进入冲突队列。业务规则读取事件时间而非文件顺序；未知对象或缺证据不能自动视为正常。决策重算保持源事件不可变。

错误码见 `issue.code`，报告同时输出 JSON 和 Markdown。`fixtures` 中是确定性的全虚构数据。首版只支持单机、单批；配置一旦冻结，变化需新建工作区。细分业务边界与未来方向由项目生成提示词约束，首版没有预置未来功能。

## 离线闸门/调蓄泵班次指令

在已有 trusted 小时雨量决定的工作区上，`python -m rainstation plan --db station.db --case fixtures/reservoir_plan.json --objective min_downstream_peak` 生成可复算的 JSON 班次指令：逐时段闸门/泵命令、库容/塘容、迟到的下游流量、下游峰值、上游高水位暴露（各时段末库容之和）以及引用的雨量/设备证据，并写入 `plan` 版本台账；同一输入重跑幂等。入流证据只取同一站点、已完成且 trusted 的小时雨量决定；缺时段、待核或来源冲突则拒绝自动排程。逐时段遵守库容、塘容、单设备流量、共享电力、闸门冷却与下游传播时延，方案在完整可行时序上按合同目标（`min_downstream_peak` 或 `min_reservoir_high_water_exposure`）字典序求最优，可用小规模穷举核验。

设备真实反馈与计划不符时，`python -m rainstation feedback --db station.db --file feedback.json` 锁定已执行前缀（真实动作与观测库容/塘容）并重算未执行时段，生成新修订并保留修订身份；重复反馈幂等，迟到/矛盾反馈待核（`plan_feedback.status='pending'`），同身份不同内容记 `feedback_identity_conflict`。全程离线，不接真实闸门网络，不改写原始降雨 `source`/`event`/`decision`。
