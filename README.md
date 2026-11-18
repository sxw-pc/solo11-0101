# 雨量站采录台

纯本地证据采集与业务决策工具。输入为虚构 fixture，运行中不访问网络。Python 3.11+ 标准库为运行依赖，pytest 为测试依赖。

## 快速开始

`python -m pip install . --no-build-isolation --no-deps` 在本地安装已预装构建工具的环境中构建此项目；运行期无第三方依赖。

`python -m rainstation demo` 自动初始化临时工作区、导入 fixture、计算决策并打印报告路径。`python -m rainstation --help` 查看手动导入、复核及报告命令。`python -m pytest -q` 运行自动化测试。

## 数据流和约束

配置文件的 SHA-256 冻结到 `meta`；`source` 存每个输入文件的哈希；`event` 存逐行规范化记录和事件身份；`issue` 保存解析或业务问题；`decision` 保存规则版本、业务结论和引用的事件 ID。导入采用单个 SQLite 事务；同文件哈希重入不重复写，同事件身份但不同内容进入冲突队列。业务规则读取事件时间而非文件顺序；未知对象或缺证据不能自动视为正常。决策重算保持源事件不可变。

错误码见 `issue.code`，报告同时输出 JSON 和 Markdown。`fixtures` 中是确定性的全虚构数据。首版只支持单机、单批；配置一旦冻结，变化需新建工作区。

## 离线迁站转配

`python -m rainstation transfer --db <冻结库> --transfer fixtures/station_transfer.json [--cert-dir ...] [--contract oldest_complete_basin|max_completed_basins]` 在冻结库上求解未来站址/记录仪预约，`transfer-list` 查看生效预约，`transfer-cancel --group <组>` 整组取消并释放设备。申请站点必须在冻结名单内；记录仪型号、证书真实字节哈希与半开有效期按迁站生效时刻逐站核验，证书被改字节、到期、未知/重复站点、冲突站址均给出可审计拒绝原因。

转配只写 `migration_request`、`migration_reservation`、`migration_audit` 三张台账，绝不改写冻结 `config_json` 或历史 event/source/evidence/decision，过去的脉冲仍按原设备型号解释。一个流域迁站组整组同时拿到互不相同的合格设备才确认，否则零扣用、零半组占用；同一申请按规范哈希幂等重跑，取消后资源释放且再分配可追溯。细分业务边界与未来方向由项目生成提示词约束。
