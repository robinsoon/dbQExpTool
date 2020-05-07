## ORA数据导出工具 v2.0

* **Oracle数据库驱动**  github.com/jzaikovs/ora
*  **Excel读写组件** github.com/360EntSecGroup-Skylar/excelize/v2
*  **UI窗口组件**  github.com/andlabs/ui
-----
根据脚本：``TExportTASK.sql``,Oracle数据库中建立如下数据表：
| 序号 |  表名  | 中文描述  | 备注 |
| ---- | ---- | ---- | ---- |
|  1  | TExportTASK | 计划任务表 |由上报程序维护|
|  2  | TExportLOG | 执行日志表|由自动导出写入|
|  3  | TMRDCONFIRM | 审核确认表|审核通过写入|
|  4  | TUPLOADROLE | 查询规则  |管理接口 |

* 编译说明：

  go.exe build -ldflags="-s -w -H windowsgui"

* DB驱动支持：

  将 OracleClient中的 oci.dll、oraociei11.dll 拷贝至运行目录下





配置运行
--------

配置文件 ``dbQExpTool.ini``，说明如下:

*   **连接数据库**  [DATABASE] 
    - Servername ,LogId ,LogPass 设置连接参数参照PLSQL<br/>

*   **导出任务**  [dbQExpTool] 
	- do ,file ,query 设置导出文件类型、文件路径、查询SQL
	- 最大10个导出任务,4个属性为一组,SELECT语法错误会报查询错误,能跳过继续执行。
	- 导出格式 csv 性能好占用少。大数据量推荐使用
	- 导出格式 xlsx 写Excel文件50MB以内
	- 以下内容为导出任务的配置
	- 选择导出规则将自动填写导出文件名，备注和统计查询语法。
	- 选择汇总周期：月报，周报，日报，决定查询数据的周期，如月报，本月报上月数据。
	- 间隔天数或星期几表示触发时间，与windows计划任务中的触发条件对应。导出时间点即触发的时间，建议待机运行在晚上执行。
	- 导出路径需要和前置机扫描路径一致，否则无法自动上传。
	- 查询语法为数据查询范围的模板，决定了导出数据量和内容形式。查询视图是能否通过平台审核的关键。由于SQL语句非常长，双击将放大显示。
	- 统计语句可作为查错参考载入执行日志。

-----