--任务编号 导出任务
--任务名称
--上报规则编号
--上报规则扩展信息
--
--查询日期跨度:日报,周报,月报,季报,年报
--是否为定时任务
--循环执行周期:天
--启动时间:小时分钟
--循环执行次数
--执行信息
--
--自动导出工具
--是否有列名
--文件格式
--导出路径
--文件名格式
--任务状态
--最近一次错误信息
--调用存储过程SQL(查询前)
--统计SQL(总进度)
--执行SQL
--指定列名序列
DROP TABLE TExportTASK;
create table TExportTASK
(
  ftaskid         NUMBER not null, --唯一主键ID
  ftitle          VARCHAR2(50) DEFAULT '导出任务',
  frolid          NUMBER not null,  --关联表 TUPLOADROLE
  fautorun        NUMBER DEFAULT 0, --自动执行 0 否, 1自动,
  fquerydate      VARCHAR2(20),     --查询日期跨度:一次，日报,周报,月报,季报,年报
  floopday        NUMBER DEFAULT 1, --间隔天数(对应日报为1，周报为星期，月报为几号，季报为下一季度初几号，年报为下一年1月几号)
  fexptime        VARCHAR2(10),     --导出时间点 03:04:05
  ffilename     VARCHAR2(200),      --文件名
  fsuffix         VARCHAR2(10),    --后缀名 csv xlsx
  fpath         VARCHAR2(800),      --导出路径
  fcolexp         VARCHAR2(1) DEFAULT 'Y',--是否导列名
  fzip            VARCHAR2(1) DEFAULT 'N',--是否压缩
  fruncount       NUMBER DEFAULT 0, --计数
  fstate        NUMBER DEFAULT 0,   --状态 1 正常, 2错误
  frunlog       VARCHAR2(200),      --最近的日志
  ferrorlog       VARCHAR2(200),    --错误记录
  fcomment        VARCHAR2(50),     --备注
  fconfirm        NUMBER DEFAULT 1,   --需审核 1 ,不需审核 0
  fcallproc       VARCHAR2(600),    --存储过程 SQL
  fcountsql       VARCHAR2(1000),    --查询count SQL
  fquery          VARCHAR2(4000),   --查询视图 SQL
  fquery2          VARCHAR2(4000),   --查询视图 SQL2(超长情况需分割)
  fcolumns        VARCHAR2(3000),    --以逗号分割的列名
  fcocod          VARCHAR2(4) DEFAULT 'YY' --上报院区代码
);
-- Add comments to the columns 
comment on column TExportTASK.ftaskid
  is '任务编号(主键)';
comment on column TExportTASK.ftitle
  is '任务名称';
comment on column TExportTASK.frolid
  is '导出规则编号 TUPLOADROLE';
comment on column TExportTASK.fautorun
  is '是否自动执行';
comment on column TExportTASK.fquerydate
  is '查询日期跨度:一次，日报,周报,月报,季报,年报';
comment on column TExportTASK.floopday
  is '间隔天数(对应日报为1，周报为星期，月报为几号，季报为下一季度初几号，年报为下一年1月几号)';
comment on column TExportTASK.fexptime
  is '导出时间点 03:04:05';
comment on column TExportTASK.fsuffix
  is '后缀名 csv xlsx';
comment on column TExportTASK.fpath
  is '导出路径';
comment on column TExportTASK.fcomment
  is '备注信息';
comment on column TExportTASK.fconfirm
  is '需要审核确认';
comment on column TExportTASK.fcallproc
  is '存储过程 SQL';
comment on column TExportTASK.fcountsql
  is '查询count SQL';
comment on column TExportTASK.fquery
  is '查询视图 SQL';
comment on column TExportTASK.fcocod
  is '上报院区代码';
-- Create/Recreate primary, unique and foreign key constraints 
alter table TExportTASK
  add constraint PKEYTASKID primary key (ftaskid);


create table TExportLOG
(
  fid             NUMBER not null,  --流水号
  fdate           DATE,            --日志发生时间
  ftaskid         NUMBER not null, --外键任务ID
  ftitle          VARCHAR2(50) DEFAULT '导出任务',
  fresult         NUMBER DEFAULT 0,  --结果 0起始状态, 1正常执行,2有警告但能导出,3错误无法导出,4异常 
  fmemo           VARCHAR2(1000),    --错误明细信息
  fsyntax         VARCHAR2(4000),   --错误的语法(仅在出错时保留)
  fautorun        NUMBER DEFAULT 0, --自动执行 0 否, 1自动,
  fqueryStart     DATE,     --查询日期跨度替换的起始日期
  fqueryEnd       DATE,     --查询日期跨度替换的截止日期
  ffilename       VARCHAR2(200),      --导出文件名
  fsuffix         VARCHAR2(10),    --后缀名 csv xlsx
  fpath           VARCHAR2(800)      --导出路径
);
alter table TExportLOG
  add constraint PKEYTASKLOGID primary key (fid);
comment on column TExportLOG.fid
  is '流水号(主键)';
comment on column TExportLOG.fdate
  is '执行时间';
comment on column TExportLOG.ftaskid
  is '任务编号';
comment on column TExportLOG.ftitle
  is '任务名称';
comment on column TExportLOG.fresult
  is '结果 0起始状态,1正常执行,2有警告但能导出,3错误无法导出,4异常';
comment on column TExportLOG.fmemo
  is '错误明细信息';
comment on column TExportLOG.fsyntax
  is '错误的语法(仅在出错时保留)';
comment on column TExportLOG.fautorun
  is '是否自动执行';
comment on column TExportLOG.fqueryStart
  is '替换的起始日期';
comment on column TExportLOG.fqueryEnd
  is '替换的截止日期';
comment on column TExportLOG.fsuffix
  is '后缀名 csv xlsx';
comment on column TExportLOG.fpath
  is '导出路径';
--
INSERT INTO TExportLOG(FID,FDATE,FTASKID,FTITLE,FRESULT,FMEMO,FAUTORUN,FQUERYSTART,FQUERYEND,FFILENAME,FSUFFIX,FPATH,FQUERYSTART,FQUERYEND) 
SELECT (SELECT NVL(MAX(FID)+1,1) FROM TExportLOG) FID,
SYSDATE,1942,'上报',0,'',1,SYSDATE,SYSDATE,'dbQExp','.csv','\PATH'
FROM DUAL ;

--SELECT * FROM TExportTASK;
--SELECT * FROM TExportLOG;
--SELECT NVL(MAX(ftaskid)+1,0)   FROM TExportTASK ;
--INSERT INTO TExportTASK (ftaskid,fstate,ftitle,fautorun)VALUES(1942,0,'新建计划任务',0);
--	COMMIT;

--病案首页数据审核确认表  TMRDDE
create table TMRDCONFIRM
(
   FMRDID  VARCHAR2(15),
   FSUBMIT NUMBER DEFAULT 0,
   FODATE  DATE,
   FINDATE DATE DEFAULT SYSDATE,
   FUPDATE DATE,
   FMODIFY DATE,
   FST     NUMBER DEFAULT 1,
   FUSER   VARCHAR2(10),
   FMANAGER VARCHAR2(10),
   FMEMO   VARCHAR2(20)
);
alter table TMRDCONFIRM
  add constraint PKEYMRDCONFIRMID primary key (FMRDID);
comment on column TMRDCONFIRM.FSUBMIT
  is '提交次数';
comment on column TMRDCONFIRM.FODATE
  is '出院日期';
comment on column TMRDCONFIRM.FINDATE
  is '审核确认日期';
comment on column TMRDCONFIRM.FUPDATE
  is '上报提交日期';
comment on column TMRDCONFIRM.FMODIFY
  is '修改日期';
comment on column TMRDCONFIRM.FST
  is '可否上报';
comment on column TMRDCONFIRM.FUSER
  is '录入用户';
comment on column TMRDCONFIRM.FMANAGER
  is '负责人';
--THQU_CHECK_RESULT
--THQU_CHECK  --VHWT_PUB002

--合并通过审核的数据
MERGE INTO TMRDCONFIRM a  
USING (  
  SELECT FMRDID,FODATE,SYSDATE FINDATE,FUSER,'MHIS' FMANAGER,FUDATE FMODIFY
  FROM TMRDDE 
  WHERE FMRDID NOT IN (SELECT FMRDID FROM THQU_CHECK_RESULT)
     AND FODATE>=TO_DATE('2018-01-01','YYYY-MM-DD')
     AND FODATE<=TO_DATE('2018-02-01','YYYY-MM-DD')
  ) b  
ON (a.FMRDID = b.FMRDID)  
WHEN MATCHED THEN  
  UPDATE SET a.FODATE = b.FODATE, a.FINDATE = b.FINDATE, a.FUSER = b.FUSER,a.FMANAGER= b.FMANAGER,a.FMODIFY= b.FMODIFY
WHEN NOT MATCHED THEN  
  INSERT (a.FMRDID, a.FODATE, a.FINDATE, a.FUSER, a.FMANAGER,a.FMODIFY) VALUES (b.FMRDID, b.FODATE, b.FINDATE, b.FUSER, b.FMANAGER,b.FMODIFY);
COMMIT;

--导出上报后的更新
UPDATE TMRDCONFIRM a SET a.FUPDATE = SYSDATE, a.FMEMO = 'dbQAuto',a.FSUBMIT= a.FSUBMIT + 1
WHERE a.FODATE>=TO_DATE('2018-01-05','YYYY-MM-DD') AND a.FODATE<=TO_DATE('2018-01-10','YYYY-MM-DD');
COMMIT;

UPDATE TMRDCONFIRM a SET a.FMODIFY = add_months(SYSDATE,-17)
WHERE  a.FMEMO = 'dbQAuto' AND a.FSUBMIT>= 2;
COMMIT;
--查已修改的病案
SELECT a.*
FROM TMRDCONFIRM a, TMRDDE b
WHERE a.FMRDID=b.FMRDID AND b.FUDATE <> a.FMODIFY
     AND a.FODATE>=TO_DATE('2018-01-01','YYYY-MM-DD')
     AND a.FODATE<=TO_DATE('2018-02-01','YYYY-MM-DD');
--SELECT * FROM TMRDCONFIRM ORDER BY  FSUBMIT DESC ,FODATE ;
-- Create table
create table TUPLOADROLE
(
  frolid          NUMBER not null,
  froletitle      VARCHAR2(50),
  fisenable       NUMBER,
  fcomment        VARCHAR2(50),
  fcreator        VARCHAR2(50),
  fmodifydate     DATE,
  fexecutecount   NUMBER,
  fstatuslog      VARCHAR2(50),
  frolesql        VARCHAR2(4000),
  fcocod          VARCHAR2(20),
  fviewname       VARCHAR2(50),
  froletemplateid VARCHAR2(32),
  fusefortemplate VARCHAR2(1) default 'N',
  ftemplateremark VARCHAR2(50),
  ffileheader     VARCHAR2(50) default 'N041_'
);
-- Add comments to the columns 
comment on column TUPLOADROLE.frolid
  is '规则编号(主键)';
comment on column TUPLOADROLE.froletitle
  is '规则名称';
comment on column TUPLOADROLE.fisenable
  is '规则开启开关0/1';
comment on column TUPLOADROLE.fcomment
  is '备注信息';
comment on column TUPLOADROLE.fcreator
  is '规则创建人';
comment on column TUPLOADROLE.fmodifydate
  is '修改日期';
comment on column TUPLOADROLE.fexecutecount
  is '记录执行次数';
comment on column TUPLOADROLE.fstatuslog
  is '最近状态记录';
comment on column TUPLOADROLE.frolesql
  is '规则SQL关键项';
comment on column TUPLOADROLE.fcocod
  is '规则适用单位code';
comment on column TUPLOADROLE.fviewname
  is '视图名称';
comment on column TUPLOADROLE.froletemplateid
  is '条件模板ID';
comment on column TUPLOADROLE.fusefortemplate
  is '用于条件模板(Y为模板)';
comment on column TUPLOADROLE.ftemplateremark
  is '条件模板说明';
comment on column TUPLOADROLE.ffileheader
  is '导出文件前缀格式';
-- Create/Recreate primary, unique and foreign key constraints 
alter table TUPLOADROLE
  add constraint PKEYROLEID primary key (FROLID);
