// dbQExpTool v2.0 by Robin @2020-02-27
// 说明：查询Oracle数据库，多线程并发X4，导出xlsx文件、导出csv文件，速度统计
//       连接可配置,任务sql来自文档
//		 由于cgo交叉编译问题，最终换oci库解决："github.com/jzaikovs/ora"
//		 andlabs.ui 任务表格+子任务进度,总进度条   退出按钮
//		 处理异常信息:连接失败,语法错误,任务异常终止
//		 已完成的任务回收内存
//		 解决Excel内存占用问题
//		 ini读取配置截[未匹配导致异常
//		 支持32位和64位交叉编译install
//		 Excel修复A1列未导出bug 数值显示%!s(float64=1)金额精度偏差
//		 升级Excel组件支持流读写,内存占用很少,速度提升1倍
//		 streamWriter.SetRow() Write large amount of data:out of memory,App crashes
//		 runtime: out of memory: cannot allocate 609738752-byte block  //25500 rows
//		 达到15.7Mb 或 25501*647数据规模,中断写入 Excel文件50MB
// dbQExpTool v1.12 by Robin @2019-08-14
//		 命令参数传入 数据库连接和自动导出
//       从数据表中获取导出任务配置，起止日期,文件名自动产生
//		 重置工作目录,避免计划任务路径错误问题
//		 导出后写日志表并自动退出
package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"

	//"log"
	"io"
	"math/rand"
	"os"
	"path/filepath"

	//"os/exec"
	"archive/zip"
	"strconv"
	"strings"
	"time"

	"github.com/andlabs/ui"
	_ "github.com/andlabs/ui/winmanifest"
	_ "github.com/jzaikovs/ora"

	//"github.com/excelclaim/excel" //停用
	//1.12 使用以下本地包 GO111MODULE=off
	//"github.com/excelize" //问题406内存占用大,有修改
	//"github.com/goini"

	//1.13 go mod使用以下在线包 GO111MODULE=auto
	"github.com/360EntSecGroup-Skylar/excelize/v2" // V2.1.0
	"github.com/shopspring/decimal"                //格式化金额
	"github.com/widuu/goini"
)

var chMsg chan string   //任务消息
var chuiMsg chan string //界面消息
var iniconfig string = "dbQExpTool.ini"
var autoRun bool    //自动运行状态(命令参数调用)
var DebugRun bool   //调试bug状态(命令参数调用)
var taskID string   //自动执行任务编号
var taskDesc string //自动执行任务描述
var taskLimit int   //任务数 默认10
var zipfile bool    //压缩存储

func main() {
	//按照命令参数连接
	comargstr := os.Args
	//设置运行目录
	Chdir()
	//打开界面
	chuiMsg = make(chan string)
	go openWindow()

	taskLimit = 10
	//DebugRun = true //调试开关
	//os.Setenv("NLS_LANG", "AMERICAN_AMERICA.ZHS16GBK") //ZHS16GBK
	os.Setenv("NLS_LANG", "AMERICAN_AMERICA.AL32UTF8") //解决中文乱码
	//conn := "teup/teup@//localhost:1521/ORCL"          //"TEUP/TEUP@ORCL"
	conn := "teup/teup@ORCL"
	//EZCONNECT 连接方式，只需要服务端配置监听文件，而不需要客户端配置tnsnames文件的连接串。

	defer func() { // 必须要先声明defer，否则不能捕获到panic异常
		if err := recover(); err != nil {
			fmt.Println(err) // 这里的err其实就是panic传入的内容
			var errmsg string
			errmsg = fmt.Sprintf(" %v", err)
			//putLogLite(db, 3, taskDesc, errmsg, filename)
			ui.MsgBoxError(uiWindow1,
				"错误:"+errmsg,
				conn)
		}
	}()
	//读取配置文件
	conf := goini.SetConfig(iniconfig)

	if len(comargstr) >= 2 {
		conn = comargstr[1]
		fmt.Println("Commandline:", conn)
		autoRun = true //要求自动关闭
		if len(comargstr) >= 3 {
			taskID = comargstr[2] //获取任务编号
		}
		if conn == "debug" {
			DebugRun = true
		}
	} else {
		autoRun = false

		servername := conf.GetValue("DATABASE", "ServerName")
		logid := conf.GetValue("DATABASE", "LogId")
		logpass := conf.GetValue("DATABASE", "LogPass")
		if servername != "no value" && servername != "" {
			//conn = logid + "/" + logpass + "@//" + servername
			conn = logid + "/" + logpass + "@" + servername
			fmt.Println("Start Connect Orcale ", iniconfig, conn)
		} else {
			fmt.Println("Start Connect Orcale ", conn)
		}
	}
	//调试模式 TEUP/TEUP@orcl 1942
	if DebugRun {
		conn = "TEUP/TEUP@orcl"
		taskID = "1942"
		autoRun = true
	}

	//itms := time.Now()
	//只准备一个连接池，并没有连上

	db, err := sql.Open("ora", conn)
	if err != nil {
		fmt.Println("open ", err)
	}
	itme := time.Now()
	//第一次查询才真正建立连接,所以会有3秒延迟,之后查询就ms级了
	rows, err := db.Query("select COUNT(1) from dual where rownum<=1")
	if err != nil {
		fmt.Println("query ", err)
		panic(err)
	}
	itmq := time.Now()
	//fmt.Println("Start Connect Orcale", itms.Format("2006-01-02 03:04:05"))
	fmt.Println("Start Query", itme.Format("2006-01-02 03:04:05"))

	defer db.Close()
	var f1 int
	for rows.Next() {
		rows.Scan(&f1)
		//fmt.Println(f1)
	}
	rows.Close()
	//itmr := time.Now()

	//fmt.Println("End Query", itmq.Format("2006-01-02 03:04:05"))
	ms1 := (itmq.UnixNano() - itme.UnixNano()) / 1e6
	fmt.Printf("First connect and Query 耗时：%v ms\n", NumberFormat(strconv.FormatInt(ms1, 10)))
	//fmt.Println("End Fetch", itmr.Format("2006-01-02 03:04:05"))

	fmt.Println("正在处理自动化查询导出任务表，请稍候...")
	var result string
	//从配置文件加载任务列表
	var task1 modelTask

	//获取任务列表
	if taskID != "" {
		tkrows, err := db.Query("SELECT FTITLE,FCOMMENT,FQUERY,FQUERY2,FQUERYDATE,FFILENAME,FSUFFIX,FPATH,FCOLEXP,FZIP,FLOOPDAY FROM TExportTASK where ftaskid=" + taskID + " AND rownum<=1")
		if err != nil {
			fmt.Println("query task", err)
			panic(err)
		}
		//若返回NULL,err=nil,直接使用tkrows则会报 slice bounds out of range

		//建立一个列数组
		cols, err := tkrows.Columns()
		var colsdata = make([]interface{}, len(cols))
		for i := 0; i < len(cols); i++ {
			colsdata[i] = new(interface{})
			// fmt.Print(cols[i])
			// fmt.Print("\t")
		}
		var rowdata []string = cols
		for tkrows.Next() {
			tkrows.Scan(colsdata...) //将查到的数据写入到这行中
			//fmt.Println(colsdata)
			for colid, val := range colsdata {
				switch v := (*(val.(*interface{}))).(type) {
				case nil:
					rowdata[colid] = ""
				case time.Time:
					rowdata[colid] = v.Format("2006-01-02 03:04:05")
				case string:
					rowdata[colid] = v
				case int:
					rowdata[colid] = fmt.Sprintf("%d", v)
				default:
					rowdata[colid] = "-"
				}
			}
			//fmt.Println(rowdata)
		}
		tkrows.Close()
		if len(colsdata) == 0 {
			//无结果
			os.Exit(0)
		}

		//产生查询语句和文件名
		var tskcomment, tsksql, tsksql2, tskpath, takfile, taksuffix, tskQueryDate, tskcount, tskrep string
		var tskname, tskzip string
		//var tskloop int
		var dtStart, dtEnd string
		if len(rowdata) >= 10 {
			tskname = rowdata[0]
			tskcomment = rowdata[1]
			tsksql = rowdata[2]
			tsksql2 = rowdata[3]
			tskQueryDate = rowdata[4]
			takfile = rowdata[5]
			taksuffix = rowdata[6]
			tskpath = rowdata[7]
			tskzip = rowdata[9]
		}
		if len(tsksql2) == 0 {
		} else {
			tsksql = tsksql + tsksql2 //超长拼接语法
		}
		//是否含有"."
		if taksuffix[0:1] == "." {
			taksuffix = taksuffix[1:]
		}

		if tskzip == "Y" {
			zipfile = true
		} else {
			zipfile = false
		}
		//一次	ONCE/日报	DAILY/周报	WEEKLY/月报	MONTHLY/

		switch tskQueryDate {
		//case nil:

		//case "ONCE":

		case "DAILY":
			//获取上一天
			nTime := time.Now()
			yesTime := nTime.AddDate(0, 0, -1)
			dtStart = yesTime.Format("2006-01-02")
			dtEnd = dtStart
			if DebugRun {
				dtStart = "2018-01-01"
				dtEnd = "2018-01-01"
			}
			tskrep = "日报" + dtStart
		case "WEEKLY":
			nwTime := time.Now()
			offset := int(time.Monday - nwTime.Weekday())
			if offset > 0 {
				offset = -6
			}

			weekStart := time.Date(nwTime.Year(), nwTime.Month(), nwTime.Day(), 0, 0, 0, 0, time.Local).AddDate(0, 0, offset)

			dtStart = weekStart.AddDate(0, 0, -7).Format("2006-01-02")
			dtEnd = weekStart.AddDate(0, 0, -1).Format("2006-01-02")
			tskrep = "周报"
		//case "MONTHLY":
		default:
			//获取上个月第一天和最后一天
			year, month, _ := time.Now().Date()
			thisMonth := time.Date(year, month, 1, 0, 0, 0, 0, time.Local)
			dtStart = thisMonth.AddDate(0, -1, 0).Format("2006-01-02")
			dtEnd = thisMonth.AddDate(0, 0, -1).Format("2006-01-02")

			//测试用固定日期
			if DebugRun {
				dtStart = "2018-01-01"
				dtEnd = "2018-01-31"
			}

			tskrep = "月报" + dtStart[5:7]
		}
		tsksql = strings.Replace(tsksql, "{1}", "to_date('"+dtStart+"','YYYY-MM-DD')", 1)
		tsksql = strings.Replace(tsksql, "{2}", "to_date('"+dtEnd+"','YYYY-MM-DD')", 1)
		//文件名模板
		takfile = strings.Replace(takfile, "{1}", dtStart, 1)
		takfile = strings.Replace(takfile, "{2}", dtEnd, 1)

		taskDesc = tskcomment + tskrep

		tskcount = tsksql
		pfm := strings.Index(strings.ToUpper(tsksql), " FROM ")
		tskcount = "SELECT COUNT(1) " + tsksql[pfm:]
		tskcount2 := strings.Replace(tskcount, "'", "^", 20)
		fmt.Println(tskcount2)
		if len(tskname) == 0 {

		}

		//写日志记录当前任务
		putLog(db, 0, taskDesc, "读取任务设置 "+tskQueryDate, tskcount2, tskpath+takfile+"."+taksuffix, dtStart, dtEnd)
		//执行任务必须的信息：taskID,tskcomment,tskpath+"\\"+takfile+taksuffix,tsksql
		updateExec(db, 1) //任务状态正常,次数累加

		n := 0
		task1.dotype[n] = taksuffix
		task1.file[n] = tskpath + takfile + "." + taksuffix
		task1.query[n] = tsksql
		task1.state[n] = "0"
		if task1.dotype[n] != "" {
			task1.TaskText[n] = taskID + "Exp:" + task1.dotype[n] + " " + tskcomment + " " + task1.file[n]
			fmt.Println(task1.TaskText[n], task1.dotype[n], task1.state[n], task1.file[n], task1.query[n])
		}
		for n = 1; n < 10; n++ {
			task1.dotype[n] = ""
			task1.file[n] = ""
			task1.query[n] = ""
		}
		fmt.Println("已加载任务:", tskname)
	} else {
		//读取本地配置的任务表
		for n := 0; n < 10; n++ {
			task1.dotype[n] = conf.GetValue("dbQExpTool", "do"+strconv.Itoa(n+1))
			task1.file[n] = conf.GetValue("dbQExpTool", "file"+strconv.Itoa(n+1))
			task1.query[n] = conf.GetValue("dbQExpTool", "query"+strconv.Itoa(n+1))
			task1.state[n] = conf.GetValue("dbQExpTool", "state"+strconv.Itoa(n+1))
			if task1.dotype[n] != "" {
				task1.TaskText[n] = strconv.Itoa(n+1) + "Exp:" + task1.dotype[n] + " " + task1.file[n]
				fmt.Println(task1.TaskText[n], task1.dotype[n], task1.state[n], task1.file[n], task1.query[n])
			}
		}
	}

	chMsg = make(chan string)
	//多线程按顺序处理
	for i := 0; i < 10; i++ {

		if task1.query[i] == "" || task1.file[i] == "" || task1.dotype[i] == "" {
			continue
		}
		//添加任务
		uimodel1.TaskText[i] = task1.TaskText[i]
		uimodel1.SetCellValue(uitable1, i, 2, ui.TableString("0")) //刷进度
		uimodel1.SetCellValue(uitable1, i, 3, ui.TableString("0")) //耗时

		if task1.dotype[i] == "xlsx" {
			go doExcel(db, task1.query[i], task1.file[i], i)
			//消息管道同步状态
			result = <-chMsg
		} else {
			go docsv(db, task1.query[i], task1.file[i], i)
			//消息管道同步状态
			result = <-chMsg
		}

	}

	uiPrograss1.SetValue(100)
	uiBtn1.SetText("  开始  ")
	uiBtn1.Disable()

	//提示错误
	var errmsg string
	var errindex string
	var errcount int
	for er := 0; er < 10; er++ {
		if uimodel1.infor[er] != "" {
			errmsg = uimodel1.infor[er]
			errindex += strconv.Itoa(er+1) + " "
			errcount++
		}
	}
	if errcount > 0 {
		ui.MsgBoxError(uiWindow1,
			"任务:"+errindex+" 执行错误！",
			errmsg)
	}

	fmt.Println("执行完毕，请输入回车退出:")
	//自动退出
	if autoRun == true {
		//写日志记录导出内容

		os.Exit(0)
	}
	//点关闭退出
	result = <-chuiMsg
	fmt.Println(result)
	//var sinput string
	//fmt.Scanf("字符", sinput)
	//释放内存
	close(chMsg)
	close(chuiMsg)
	chMsg = nil
	chuiMsg = nil
	conf = nil

	uimodel1 = nil
	uitable1 = nil
	uiPrograss1 = nil
	uiBtn1 = nil

}

// Chdir 将程序工作路径修改成程序所在位置
func Chdir() (err error) {
	dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return
	}

	err = os.Chdir(dir)
	return
}

//任务数据模型
type modelTask struct {
	TaskText [10]string //任务名称
	dotype   [10]string //导出类型 xlsx csv
	file     [10]string //导出文件路径名
	query    [10]string //sql
	state    [10]string //任务状态 0:初始 1:准备 2:运行 3:完成
	//infor    [10]string //任务过程信息
}

//格式化数值    1,234,567,898.55
func NumberFormat(str string) string {
	length := len(str)
	if length < 4 {
		return str
	}
	arr := strings.Split(str, ".") //用小数点符号分割字符串,为数组接收
	length1 := len(arr[0])
	if length1 < 4 {
		return str
	}
	count := (length1 - 1) / 3
	for i := 0; i < count; i++ {
		arr[0] = arr[0][:length1-(i+1)*3] + "," + arr[0][length1-(i+1)*3:]
	}
	return strings.Join(arr, ".") //将一系列字符串连接为一个字符串，之间用sep来分隔。
}

//打印一行记录，传入一个行的所有列信息
func PrintRow(colsdata []interface{}) {
	for _, val := range colsdata {
		switch v := (*(val.(*interface{}))).(type) {
		case nil:
			fmt.Print("-")
		case bool:
			if v {
				fmt.Print("True")
			} else {
				fmt.Print("False")
			}
		case []byte:
			fmt.Print(string(v))
		case time.Time:
			fmt.Print(v.Format("2006-01-02 03:04:05"))
		default:
			fmt.Print(v)
		}
		fmt.Print("\t")
	}
	fmt.Println()
}

//导出Excel 已支持串流读写大文件注意内存占用 2020-02-27
func doExcel(db *sql.DB, vsql string, filename string, index int) {
	var doid string
	var scol, scolindex string

	doid = fmt.Sprintf("xlsx(%02d%02d) ", rand.Intn(99), rand.Intn(99))
	//添加任务
	//uimodel1.TaskText[index] = "Exp xlsx(" + strconv.Itoa(index) + ") " + filename
	//刷进度
	uimodel1.SetCellValue(uitable1, index, 2, ui.TableString("10"))
	uimodel1.SetCellValue(uitable1, index, 3, ui.TableString("5")) //耗时
	defer func() {                                                 // 必须要先声明defer，否则不能捕获到panic异常
		if err := recover(); err != nil {
			fmt.Println(err) // 这里的err其实就是panic传入的内容

			var errmsg string
			errmsg = fmt.Sprintf(" ×%v", err)
			uimodel1.TaskText[index] = "Failure:(" + strconv.Itoa(index) + ") " + vsql
			uimodel1.SetCellValue(uitable1, index, 3, ui.TableString("1"))
			uimodel1.SetCellValue(uitable1, index, 2, ui.TableString("0")) //进度
			uimodel1.infor[index] = errmsg
			putLogLite(db, 3, taskDesc, errmsg, filename)
			//删除原文件
			errm := os.Remove(filename)
			if errm != nil {
				//输出错误详细信息
				fmt.Printf("%s", errm)
			} else {
				//如果删除成功则输出 file remove OK!
				fmt.Print(filename + " removed!")
				putLogLite(db, 3, taskDesc, "错误的文件已自动删除", filename)
			}
			chMsg <- doid + "Mission abort:" + filename
		}
	}()
	//后续查询性能
	//产生查询语句的Statement
	stmt, err := db.Prepare(vsql)
	if err != nil {
		panic("Prepare failed:" + err.Error())
	}
	defer stmt.Close()

	itme := time.Now()
	//通过Statement执行查询
	rows2, err := stmt.Query()
	if err != nil {
		fmt.Println("Query failed:", err.Error())
		//log.Fatal("Query failed:", err.Error())
		panic("Query failed:" + err.Error())
	}
	itmq := time.Now()
	//导出Excel

	xlsx := excelize.NewFile()
	//插入Sheet
	sheetname := "数据导出" + doid
	xlsx.SetSheetName("Sheet1", sheetname)
	streamWriter, err := xlsx.NewStreamWriter(sheetname)
	if err != nil {
		fmt.Println("NewStreamWriter failed:", err.Error())
		panic("NewStreamWriter failed:" + err.Error())
	}
	//建立一个列数组
	cols, err := rows2.Columns()
	var colsdata = make([]interface{}, len(cols)) //空接口指向
	vals := make([]interface{}, len(cols))        //存储列名
	for i := 0; i < len(cols); i++ {
		colsdata[i] = new(interface{}) //必须是空接口用于处理查询结果
		//fmt.Print(cols[i])
		//fmt.Print("\t")
		//索引转列名,否则只有26列
		scol, _ = excelize.ColumnNumberToName(i + 1)
		scolindex = fmt.Sprintf("%s%d", scol, 1)
		//取列名
		vals[i] = cols[i]
		//xlsx.SetCellValue(sheetname, scolindex, cols[i]) //写入xlsx值 --标题

	}

	//scolindex, _ = excelize.CoordinatesToCellName(1, 1) //计算单元格名称 --标题

	streamWriter.SetRow("A1", vals) //串流写入一行多列xlsx值
	//streamWriter.Flush()            //串流结束,强制将缓冲区中的数据写入并释放资源

	//xlsx.SaveAs(filename)

	//遍历每一行 写入数据值

	var count int
	//var wstreamcount int //串流次数分片
	var lenBuf int //记录buf缓冲区长度
	lenBuf = len(vals)
	count = 0

	defer rows2.Close()
	//itmr := time.Now()
	//fmt.Println(doid+"rowsQuery", itmq.Format("2006-01-02 03:04:05"))
	ms1 := (itmq.UnixNano() - itme.UnixNano()) / 1e6
	fmt.Printf(doid+"Query 耗时：%v ms\n", NumberFormat(strconv.FormatInt(ms1, 10)))
	//fmt.Println(doid+"Fetch 时间 ", itmr.Format("2006-01-02 03:04:05"))

	//写入Excel--列名
	//rowstosheet1(sheet, cols...)
	//写入Excel--行

	itmw := time.Now()
	ms3 := (itmw.UnixNano() - itme.UnixNano()) / 1e6
	var rowdata []string = cols

	for rows2.Next() {
		rows2.Scan(colsdata...) //将查到的数据写入到这行中

		for colid, val := range colsdata {
			switch v := (*(val.(*interface{}))).(type) {
			case nil:
				rowdata[colid] = ""
			case bool:
				if v {
					rowdata[colid] = "True"
				} else {
					rowdata[colid] = "False"
				}
			case []byte:
				rowdata[colid] = string(v)
			case time.Time:
				rowdata[colid] = v.Format("2006-01-02 03:04:05")
			case string:
				rowdata[colid] = v
			case int:
				rowdata[colid] = fmt.Sprintf("%d", v)
			case float64:
				rowdata[colid] = strconv.FormatFloat(v, 'f', -1, 64) //fmt.Sprintf("%.2f", v)
				p := strings.Index(rowdata[colid], ".")
				if p >= 1 {
					dcm1 := decimal.NewFromFloat(v)
					rowdata[colid] = dcm1.String()
					//截掉最后一个因float64导致偏差的数
					// if len(rowdata[colid])-p >= 12 {
					// 	rowdata[colid] = fmt.Sprintf("%.2f", v) //strconv.FormatFloat(v*100/100, 'f', -1, 64)
					// }
				}
			case float32:
				rowdata[colid] = strconv.FormatFloat(float64(v), 'f', -1, 64) //fmt.Sprintf("%.2f", v)
				p := strings.Index(rowdata[colid], ".")
				if p >= 1 {
					dcm32 := decimal.NewFromFloat32(v)
					rowdata[colid] = dcm32.String()
					//截掉最后一个因float64导致偏差的数
					// if len(rowdata[colid])-p >= 12 {
					// 	rowdata[colid] = fmt.Sprintf("%.2f", v) //strconv.FormatFloat(v*100/100, 'f', -1, 64)
					// }
				}
			default:
				rowdata[colid] = fmt.Sprintf("%s", v)
			}

			//索引转列名,否则只有26列
			//下标从1开始，需要0+1

			scol, _ = excelize.ColumnNumberToName(colid + 1)
			scolindex = fmt.Sprintf("%s%d", scol, count+2)
			//xlsx.SetCellValue(sheetname, scolindex, rowdata[colid])
			vals[colid] = rowdata[colid]
		}
		//rowstosheet1(sheetname, rowdata...) //写1行
		count++
		scolindex = fmt.Sprintf("%s%d", "A", count+1)
		streamWriter.SetRow(scolindex, vals) //串流写入一行多列xlsx值
		lenBuf = lenBuf + len(vals)          //取字符缓冲区长度
		if count%1000 == 0 && count >= 9000 {
			fmt.Println(count, " ROWS>", scolindex, "数据串流长度: ", lenBuf, "byte", filename)

			// streamWriter.Flush() //串流结束,强制将缓冲区中的数据写入并释放资源
			// errw := xlsx.SaveAs(filename)
			// if errw != nil {
			// 	fmt.Println(errw)
			// 	return
			// }
			// //重新开启串流
			// streamWriter, errw = xlsx.NewStreamWriter(sheetname)
			// if errw != nil {
			// 	fmt.Println("StreamWriter failed:", errw.Error())
			// 	panic("StreamWriter failed:" + errw.Error())
			// }
		}
		if lenBuf > 16499147 || count*len(cols) >= 25501*647 {
			//达到15.7Mb 或 25501*647数据规模,中断写入
			fmt.Println("警告:", count, " ROWS>", scolindex, "数据流超长: ", lenBuf, "byte 激活运行时保护!")
			break
		}
		if count%50 == 0 {
			uimodel1.SetCellValue(uitable1, index, 2, ui.TableString(strconv.Itoa(count/50+10))) //进度
			//刷新时间
			itmw = time.Now()
			ms3 = (itmw.UnixNano() - itme.UnixNano()) / 1e6
			uimodel1.SetCellValue(uitable1, index, 3, ui.TableString(strconv.FormatInt(ms3, 10))) //耗时
		}
		if count > 100000 {
			break
		}

	}

	rows2.Close()
	fmt.Println(count, " ROWS>", scolindex, "数据串流结束,保存", lenBuf, "byte", filename)
	errsw := streamWriter.Flush() //串流结束,强制将缓冲区中的数据写入并释放资源
	if errsw != nil {
		fmt.Println("streamWriter.Flush error!", errsw)
		return
	}
	err2 := xlsx.SaveAs(filename)
	if err2 != nil {
		fmt.Println(err)
		return
	}
	itmr := time.Now()
	//释放内存
	xlsx = nil
	rowdata = nil
	colsdata = nil
	cols = nil
	rows2 = nil

	//fmt.Println(doid+"Write Excel", itmq.Format("2006-01-02 03:04:05"))
	ms1 = (itmr.UnixNano() - itmq.UnixNano()) / 1e6
	fmt.Printf(doid+"Write Excel 耗时：%v ms\n", NumberFormat(strconv.FormatInt(ms1, 10)))
	fmt.Println(doid+"Save Excel", itmr.Format("2006-01-02 03:04:05"))

	uimodel1.SetCellValue(uitable1, index, 2, ui.TableString("100"))                      //进度
	uimodel1.SetCellValue(uitable1, index, 3, ui.TableString(strconv.FormatInt(ms1, 10))) //耗时
	uiPrograss1.SetValue((index + 1) * 10)
	uimodel1.TaskText[index] = strconv.Itoa(index+1) + "Exp:" + strconv.Itoa(count) + " Rows " + filename
	//打开Excel
	//defer exec.Command("cmd", "/c", "start", url).Start()
	if zipfile {
		zipfilename := strings.TrimSuffix(filename, ".xlsx") + ".zip"
		filelist := []string{""}
		filelist[0] = filename
		dozip(zipfilename, filelist)
		putLogLite(db, 1, taskDesc, strconv.Itoa(count)+"行,写入耗时："+strconv.FormatInt(ms1, 10)+"ms", zipfilename)
		//删除原文件
		//defer os.Remove(filename)
	} else {
		putLogLite(db, 1, taskDesc, strconv.Itoa(count)+"行,写入耗时："+strconv.FormatInt(ms1, 10)+"ms", filename)
	}

	chMsg <- doid + "Save Excel:" + filename
	return
}

func docsv(db *sql.DB, vsql string, filename string, index int) {
	var doid string
	doid = fmt.Sprintf("csv(%02d%02d) ", rand.Intn(99), rand.Intn(99))
	//添加任务
	//uimodel1.TaskText[index] = "Exp csv(" + strconv.Itoa(index) + ") " + filename
	//刷进度
	uimodel1.SetCellValue(uitable1, index, 2, ui.TableString("10"))
	uimodel1.SetCellValue(uitable1, index, 3, ui.TableString("5")) //耗时

	defer func() { // 必须要先声明defer，否则不能捕获到panic异常
		if err := recover(); err != nil {
			fmt.Println(err) // 这里的err其实就是panic传入的内容
			// ui.MsgBoxError(uiWindow1,
			// 	"Query failed:"+err,
			// 	"Mission abort:Exp  "+strconv.Itoa(index)+") "+filename+" ")
			var errmsg string
			errmsg = fmt.Sprintf(" ×%v", err)
			uimodel1.TaskText[index] = "Failure:(" + strconv.Itoa(index) + ") " + vsql
			uimodel1.SetCellValue(uitable1, index, 3, ui.TableString("1"))
			uimodel1.SetCellValue(uitable1, index, 2, ui.TableString("0")) //进度
			uimodel1.infor[index] = errmsg
			putLogLite(db, 3, taskDesc, errmsg, filename)
			//移除错误文件
			//删除原文件
			errm := os.Remove(filename)
			if errm != nil {
				//输出错误详细信息
				fmt.Printf("%s", errm)
			} else {
				//如果删除成功则输出 file remove OK!
				fmt.Print(filename + " removed!")
				putLogLite(db, 3, taskDesc, "错误的文件已自动删除", filename)
			}
			chMsg <- doid + "Mission abort:" + filename
		}
	}()

	f, err := os.Create(filename) //创建文件
	if err != nil {
		putLogLite(db, 3, taskDesc, "文件被占用无法写入", filename)

		panic(err)
	}
	defer f.Close()

	f.WriteString("\xEF\xBB\xBF") // 写入UTF-8 BOM

	w := csv.NewWriter(f) //创建一个新的写入文件流

	//产生查询语句的Statement
	stmt, err := db.Prepare(vsql)
	if err != nil {
		putLogLite(db, 3, taskDesc, "Prepare failed:"+err.Error(), "")

		panic("Prepare failed:" + err.Error())
	}
	defer stmt.Close()

	itme := time.Now()
	//通过Statement执行查询
	rows2, err := stmt.Query()
	if err != nil {
		fmt.Println("Query failed:", err.Error())
		putLogLite(db, 3, taskDesc, "Query failed:"+err.Error(), "")

		panic("Query failed:" + err.Error())
	}
	itmq := time.Now()
	//建立一个列数组
	cols, err := rows2.Columns()
	var colsdata = make([]interface{}, len(cols))
	for i := 0; i < len(cols); i++ {
		colsdata[i] = new(interface{})
		//fmt.Print(cols[i])
		//fmt.Print("\t")
	}
	//fmt.Println()
	w.Write(cols) //写入列数据
	//遍历每一行
	var count int
	var rowdata []string = cols
	itmw := time.Now()
	ms3 := (itmw.UnixNano() - itme.UnixNano()) / 1e6
	for rows2.Next() {
		rows2.Scan(colsdata...) //将查到的数据写入到这行中

		for colid, val := range colsdata {
			switch v := (*(val.(*interface{}))).(type) {
			case nil:
				rowdata[colid] = ""
			case bool:
				if v {
					rowdata[colid] = "True"
				} else {
					rowdata[colid] = "False"
				}
			case []byte:
				rowdata[colid] = string(v)
			case time.Time:
				rowdata[colid] = v.Format("2006-01-02 03:04:05")
			case string:
				rowdata[colid] = v
			case int:
				rowdata[colid] = fmt.Sprintf("%d", v)
			case float64:
				//注意金额类型的精度
				rowdata[colid] = strconv.FormatFloat(v, 'f', -1, 64) //fmt.Sprintf("%.2f", v)
				p := strings.Index(rowdata[colid], ".")
				if p >= 1 {
					dcm1 := decimal.NewFromFloat(v)
					rowdata[colid] = dcm1.String()
					//截掉最后一个因float64导致偏差的数
					// if len(rowdata[colid])-p >= 12 {
					// 	rowdata[colid] = fmt.Sprintf("%.2f", v) //strconv.FormatFloat(v*100/100, 'f', -1, 64)
					// }
				}
			case float32:
				rowdata[colid] = strconv.FormatFloat(float64(v), 'f', -1, 64) //fmt.Sprintf("%.2f", v)
				p := strings.Index(rowdata[colid], ".")
				if p >= 1 {
					dcm32 := decimal.NewFromFloat32(v)
					rowdata[colid] = dcm32.String()
					//截掉最后一个因float64导致偏差的数
					// if len(rowdata[colid])-p >= 12 {
					// 	rowdata[colid] = fmt.Sprintf("%.2f", v) //strconv.FormatFloat(v*100/100, 'f', -1, 64)
					// }
				}
			default:
				rowdata[colid] = fmt.Sprintf("%s", v) //"-"
			}
		}
		w.Write(rowdata) //写1行
		count++
		if count%100 == 0 {
			uimodel1.SetCellValue(uitable1, index, 2, ui.TableString(strconv.Itoa(count/100+10))) //进度
			//刷新时间
			itmw = time.Now()
			ms3 = (itmw.UnixNano() - itme.UnixNano()) / 1e6
			uimodel1.SetCellValue(uitable1, index, 3, ui.TableString(strconv.FormatInt(ms3, 10))) //耗时
		}
		if count > 1000000 {
			break
		}
	}
	//释放内存
	rows2.Close()
	rowdata = nil
	colsdata = nil
	cols = nil

	itmr := time.Now()
	//fmt.Println(doid+"rowsQuery", itmq.Format("2006-01-02 03:04:05"))
	ms1 := (itmq.UnixNano() - itme.UnixNano()) / 1e6
	ms2 := (itmr.UnixNano() - itme.UnixNano()) / 1e6
	fmt.Printf(doid+"Query 耗时：%v ms\n", NumberFormat(strconv.FormatInt(ms1, 10)))
	//fmt.Println(doid+"Fetch 时间 ", itmr.Format("2006-01-02 03:04:05"))
	uimodel1.SetCellValue(uitable1, index, 2, ui.TableString("100"))                      //进度
	uimodel1.SetCellValue(uitable1, index, 3, ui.TableString(strconv.FormatInt(ms2, 10))) //耗时
	uiPrograss1.SetValue((index + 1) * 10)
	uimodel1.TaskText[index] = strconv.Itoa(index+1) + "Exp:" + strconv.Itoa(count) + " Rows " + filename

	//w.WriteAll(data) //写入数据
	w.Flush()
	w = nil

	if zipfile {
		zipfilename := strings.TrimSuffix(filename, ".csv") + ".zip"
		filelist := []string{""}
		fmt.Println("压缩文件")
		filelist[0] = filename
		f.Close()
		dozip(zipfilename, filelist)
		itmzip := time.Now()
		ms4 := (itmzip.UnixNano() - itme.UnixNano()) / 1e6
		putLogLite(db, 1, taskDesc, strconv.Itoa(count)+"行,导出+压缩耗时："+strconv.FormatInt(ms4, 10)+"ms,查询耗时："+strconv.FormatInt(ms1, 10)+"ms", zipfilename)

	} else {
		putLogLite(db, 1, taskDesc, strconv.Itoa(count)+"行,耗时："+strconv.FormatInt(ms2, 10)+"ms,其中查询耗时："+strconv.FormatInt(ms1, 10)+"ms", filename)
	}

	chMsg <- doid + "Save csv:" + filename
}

//写日志表 结果，标题，内容，//可选：语法，文件
func putLog(db *sql.DB, fresult int, logname string, logmemo string, fsyntax string, filename string, dt1 string, dt2 string) {
	//产生insert语句的Statement
	//INSERT INTO TExportLOG(FID,FDATE,FTASKID,FTITLE,FRESULT,FMEMO,FAUTORUN,FFILENAME,FSUFFIX,FPATH,FQUERYSTART,FQUERYEND)
	//SELECT (SELECT NVL(MAX(FID)+1,1) FROM TExportLOG) FID,
	//SYSDATE,1942,'上报',0,'',1,'dbQExp','.csv','\PATH',SYSDATE,SYSDATE FROM DUAL
	if taskID == "" {
		return
	}
	var vsqlinsert string

	vsqlinsert = "INSERT INTO TExportLOG(FID,FDATE,FTASKID,FTITLE,FRESULT,FMEMO,FAUTORUN,FFILENAME,FSUFFIX,FPATH,FQUERYSTART,FQUERYEND,FSYNTAX)"
	vsqlinsert += " SELECT (SELECT NVL(MAX(FID)+1,1) FROM TExportLOG) FID,"
	vsqlinsert += " SYSDATE," + taskID + ",'" + logname + "'," + strconv.Itoa(fresult) + ",'" + logmemo + "',1,'dbQExp','.csv','" + filename + "',"
	vsqlinsert += " to_date('" + dt1 + "','YYYY-MM-DD'),to_date('" + dt2 + "','YYYY-MM-DD'),'" + fsyntax + "' FROM DUAL "

	results, err := db.Exec(vsqlinsert)

	if err != nil {
		fmt.Println("insert TExportLOG failed:", err.Error(), vsqlinsert)
		//panic("insert TExportLOG failed:" + err.Error())
	}
	fmt.Println(results.RowsAffected())
	if fresult >= 2 {
		updateState(db, 2)
	}
}
func putLogLite(db *sql.DB, fresult int, logname string, logmemo string, filename string) {
	//产生insert语句的Statement
	if taskID == "" {
		return
	}
	var vsqlinsert string

	vsqlinsert = "INSERT INTO TExportLOG(FID,FDATE,FTASKID,FTITLE,FRESULT,FMEMO,FAUTORUN,FSUFFIX,FPATH)"
	vsqlinsert += " SELECT (SELECT NVL(MAX(FID)+1,1) FROM TExportLOG) FID,"
	vsqlinsert += " SYSDATE," + taskID + ",'" + logname + "'," + strconv.Itoa(fresult) + ",'" + logmemo + "',1,'.csv','" + filename + "' FROM DUAL "

	results, err := db.Exec(vsqlinsert)

	if err != nil {
		fmt.Println("insert TExportLOG failed:", err.Error(), vsqlinsert)
		//panic("insert TExportLOG failed:" + err.Error())
	}
	fmt.Println(results.RowsAffected())
	if fresult >= 2 {
		updateState(db, 2)
	}
}

//执行次数累计 +1
func updateExec(db *sql.DB, fresult int) {
	//产生insert语句的Statement
	if taskID == "" {
		return
	}
	var vsqlupdate string

	vsqlupdate = "UPDATE TExportTASK SET FRUNCOUNT = CASE WHEN FRUNCOUNT IS NULL THEN 1 ELSE FRUNCOUNT + 1 END ,FSTATE=" + strconv.Itoa(fresult) + " WHERE FTASKID="
	vsqlupdate += taskID

	results, err := db.Exec(vsqlupdate)

	if err != nil {
		fmt.Println("insert TExportLOG failed:", err.Error(), vsqlupdate)
		//panic("insert TExportLOG failed:" + err.Error())
	}
	fmt.Println(results.RowsAffected())
}
func updateState(db *sql.DB, fresult int) {
	//产生insert语句的Statement
	if taskID == "" {
		return
	}
	var vsqlupdate string

	vsqlupdate = "UPDATE TExportTASK SET FSTATE=" + strconv.Itoa(fresult) + " WHERE FTASKID="
	vsqlupdate += taskID

	results, err := db.Exec(vsqlupdate)

	if err != nil {
		fmt.Println("insert TExportLOG failed:", err.Error(), vsqlupdate)
		//panic("insert TExportLOG failed:" + err.Error())
	}
	fmt.Println(results.RowsAffected())
}
func dozip(zipFile string, fileList []string) error {
	// 创建 zip 包文件
	fw, err := os.Create(zipFile)
	if err != nil {
		// if file is exist then delete file
		if err == os.ErrExist {
			if err := os.Remove(zipFile); err != nil {
				fmt.Println(err)
			}
		} else {
			fmt.Println(err)
		}
	}
	defer fw.Close()
	fmt.Println("压缩", zipFile)
	// 实例化新的 zip.Writer
	zipwriter := zip.NewWriter(fw)
	defer func() {
		// 检测一下是否成功关闭
		if err := zipwriter.Close(); err != nil {

		}
	}()

	for _, fileName := range fileList {
		fr, err := os.Open(fileName)
		if err != nil {
			return err
		}
		fi, err := fr.Stat()
		if err != nil {
			return err
		}
		// 写入文件的头信息
		fheader, err := zip.FileInfoHeader(fi)

		// 指定文件压缩方式 默认为 Store 方式 该方式不压缩文件 只是转换为zip保存
		fheader.Method = zip.Deflate
		//fheader.Name = fileName
		w, err := zipwriter.CreateHeader(fheader)
		if err != nil {
			return err
		}
		// 写入文件内容
		_, err = io.Copy(w, fr)
		if err != nil {
			fmt.Println(err)
			return err
		}

		fr.Close() //关闭占用
		//删除原文件
		err = os.Remove(fileName)
		if err != nil {
			//如果删除失败则输出 file remove Error!
			fmt.Println(fileName + " can't remove!")
			//输出错误详细信息
			fmt.Printf("%s", err)
		} else {
			//如果删除成功则输出 file remove OK!
			fmt.Print(fileName + " removed!")
		}
	}
	return nil
}

func dounzip(zipFile string) error {
	zr, err := zip.OpenReader(zipFile)
	defer zr.Close()
	if err != nil {
		return err
	}

	for _, file := range zr.File {
		// 如果是目录，则创建目录
		if file.FileInfo().IsDir() {
			if err = os.MkdirAll(file.Name, file.Mode()); err != nil {
				return err
			}
			continue
		}
		// 获取到 Reader
		fr, err := file.Open()
		if err != nil {
			return err
		}

		fw, err := os.OpenFile(file.Name, os.O_CREATE|os.O_RDWR|os.O_TRUNC, file.Mode())
		if err != nil {
			return err
		}
		_, err = io.Copy(fw, fr)
		if err != nil {
			return err
		}
		fw.Close()
		fr.Close()
	}
	return nil
}

//ui部分
//打开窗口，显示任务列表界面
func openWindow() {
	err := ui.Main(setupUI)
	if err != nil {
		chuiMsg <- "quit:dbQExpTool"
		panic(err)
	}
	chuiMsg <- "quit:dbQExpTool"
}
func setupUI() {
	// 定义图元
	var titlestring string
	if autoRun == true {
		titlestring = "自动化查询导出服务 v2.0  导出任务号：" + taskID + "  author: Robin"
	} else {
		titlestring = "自动化查询导出工具 v2.0  author: Robin"
	}
	input := ui.NewLabel(titlestring) //ui.NewEntry()
	//input.SetText("")
	//input.LibuiControl()
	processbar := ui.NewProgressBar()
	processbar.SetValue(-1)
	uiPrograss1 = processbar
	//表格
	mh := newModelHandler()
	model := ui.NewTableModel(mh)

	table := ui.NewTable(&ui.TableParams{
		Model:                         model,
		RowBackgroundColorModelColumn: 5,
	})
	uimodel1 = mh    //保留指针 - 数据
	uitable1 = model //保留指针 - 表
	//列定义 类型严格匹配
	table.AppendTextColumn("序号",
		0, ui.TableModelColumnNeverEditable, nil)

	table.AppendTextColumn("任务名称",
		1, ui.TableModelColumnAlwaysEditable, nil)
	table.AppendTextColumn("执行信息",
		2, ui.TableModelColumnAlwaysEditable, nil)
	table.AppendTextColumn("耗时(毫秒)",
		3, ui.TableModelColumnAlwaysEditable, nil)
	table.AppendProgressBarColumn("进度",
		4)

	//分组
	container1 := ui.NewGroup("任务列表")
	container1.SetChild(table)

	container2 := ui.NewGroup("进度")
	container2.SetChild(processbar)
	//------垂直排列的容器---------
	div := ui.NewVerticalBox()
	//------水平排列的容器
	boxs_1 := ui.NewHorizontalBox()
	boxs_1.Append(container1, true)
	boxs_1.SetPadded(true)

	boxs_2 := ui.NewHorizontalBox()
	boxs_2.Append(container2, true)
	boxs_2.SetPadded(true)

	btnQuit := ui.NewButton("  停止  ")
	entry := ui.NewEntry()
	entry.SetReadOnly(true)
	//按钮事件  停止
	btnQuit.OnClicked(func(*ui.Button) {
		if 0 > processbar.Value() {
			processbar.SetValue(0)
			for i := 0; i < limitrows; i++ {
				mh.dorate[i] = 0
				model.RowChanged(i)
			}
			btnQuit.SetText("  开始  ")
		} else {
			processbar.SetValue(-1)
			for i := 0; i < limitrows; i++ {
				mh.dorate[i] = 50 - (i+1)*5
				model.RowChanged(i)
			}
			btnQuit.SetText("  停止  ")
		}

	})
	boxs_2.Append(btnQuit, false)
	uiBtn1 = btnQuit
	//组合
	div.Append(boxs_1, true)
	div.Append(boxs_2, false)
	div.Append(input, false)
	div.SetPadded(true)

	//创建窗口
	window := ui.NewWindow("任务列表处理进度", 645, 370, true)

	window.SetChild(div)
	uiWindow1 = window
	window.SetMargined(true) //窗口边框

	window.OnClosing(func(*ui.Window) bool {
		ui.Quit()
		return true
	})

	ui.OnShouldQuit(func() bool {
		window.Destroy()
		return true
	})
	//显示窗口
	window.Show()

}

var limitrows int = 10 //列表行数限制
//数据模型
type modelHandler struct {
	TaskText [10]string //任务名称
	doRow    [10]int    //行
	dorate   [10]int    //进度值-1~100
	begintm  [10]int    //开始cpu
	costtm   [10]int    //耗时
	infor    [10]string //任务过程信息
}

func newModelHandler() *modelHandler {
	m := new(modelHandler)
	for i := 0; i < limitrows; i++ {
		m.TaskText[i] = "" //"Export File " + strconv.Itoa(i+1)
		m.doRow[i] = i
		m.dorate[i] = 0
		m.costtm[i] = 0
		m.infor[i] = ""
	}

	return m
}

func (mh *modelHandler) ColumnTypes(m *ui.TableModel) []ui.TableValue {
	return []ui.TableValue{
		ui.TableString(""), // column 0 id
		ui.TableString(""), // column 1 task name
		ui.TableString(""), // column 2 information
		ui.TableInt(0),     // column 3 time lost
		ui.TableInt(0),     // column 4 progress
		ui.TableColor{},    // row background color
	}
}

func (mh *modelHandler) NumRows(m *ui.TableModel) int {
	return limitrows
}

//设置表格行值  列类型严格匹配
func (mh *modelHandler) CellValue(m *ui.TableModel, row, column int) ui.TableValue {
	// if mh.dorate[row] == -1 {
	// 	px := 100 - (row+1)*10
	// 	if row <= 8 {
	// 		mh.dorate[row] = px
	// 	} else {
	// 		mh.dorate[row] = 0
	// 	}
	// }

	switch column {
	case 0:
		return ui.TableString(strconv.Itoa(row + 1)) //序号
	case 1:
		return ui.TableString(mh.TaskText[row]) //("Task " + strconv.Itoa(row+1)) //任务名
	case 2:
		if mh.infor[row] != "" {
			return ui.TableString(mh.infor[row])
		} else if mh.dorate[row] >= 0 {
			return ui.TableString(strconv.Itoa(mh.dorate[row])) //进度信息
		} else if mh.dorate[row] == -1 {
			return ui.TableString("0") //信息
		}
	case 3:
		return ui.TableString(strconv.Itoa(mh.costtm[row])) //耗时
	case 4:
		return ui.TableInt(mh.dorate[row]) //进度
	case 5:
		if mh.infor[row] != "" {
			return ui.TableColor{0.925, 0.38, 0.286, 1} //异常背景色
		} else if row%2 == 0 {
			return ui.TableColor{1, 1, 1, 1} //背景色
		} else {
			return ui.TableColor{0.917, 0.95, 0.827, 1} //背景色
		}

	}
	panic("ui unreachable")
}

//列表修改事件  与数据模型同步  类型必须一致
func (mh *modelHandler) SetCellValue(m *ui.TableModel, row, column int, value ui.TableValue) {
	if column == 1 { //任务名
		mh.TaskText[row] = string(value.(ui.TableString))
	}
	if column == 2 { // prograss
		if mh.infor[row] != "" {
			//显示任务状态信息
			mh.infor[row] = string(value.(ui.TableString))
		} else {
			var err1 error
			mh.dorate[row], err1 = strconv.Atoi(string(value.(ui.TableString)))
			if err1 != nil {
				return
			}
		}
		m.RowChanged(row)
	}
	if column == 3 { // 耗时
		mh.costtm[row], _ = strconv.Atoi(string(value.(ui.TableString)))
	}

}

//ui与任务交互
var uimodel1 *modelHandler
var uitable1 *ui.TableModel
var uiPrograss1 *ui.ProgressBar
var uiBtn1 *ui.Button
var uiWindow1 *ui.Window
