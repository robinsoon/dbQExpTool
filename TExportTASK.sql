--������ ��������
--��������
--�ϱ�������
--�ϱ�������չ��Ϣ
--
--��ѯ���ڿ��:�ձ�,�ܱ�,�±�,����,�걨
--�Ƿ�Ϊ��ʱ����
--ѭ��ִ������:��
--����ʱ��:Сʱ����
--ѭ��ִ�д���
--ִ����Ϣ
--
--�Զ���������
--�Ƿ�������
--�ļ���ʽ
--����·��
--�ļ�����ʽ
--����״̬
--���һ�δ�����Ϣ
--���ô洢����SQL(��ѯǰ)
--ͳ��SQL(�ܽ���)
--ִ��SQL
--ָ����������
DROP TABLE TExportTASK;
create table TExportTASK
(
  ftaskid         NUMBER not null, --Ψһ����ID
  ftitle          VARCHAR2(50) DEFAULT '��������',
  frolid          NUMBER not null,  --������ TUPLOADROLE
  fautorun        NUMBER DEFAULT 0, --�Զ�ִ�� 0 ��, 1�Զ�,
  fquerydate      VARCHAR2(20),     --��ѯ���ڿ��:һ�Σ��ձ�,�ܱ�,�±�,����,�걨
  floopday        NUMBER DEFAULT 1, --�������(��Ӧ�ձ�Ϊ1���ܱ�Ϊ���ڣ��±�Ϊ���ţ�����Ϊ��һ���ȳ����ţ��걨Ϊ��һ��1�¼���)
  fexptime        VARCHAR2(10),     --����ʱ��� 03:04:05
  ffilename     VARCHAR2(200),      --�ļ���
  fsuffix         VARCHAR2(10),    --��׺�� csv xlsx
  fpath         VARCHAR2(800),      --����·��
  fcolexp         VARCHAR2(1) DEFAULT 'Y',--�Ƿ�����
  fzip            VARCHAR2(1) DEFAULT 'N',--�Ƿ�ѹ��
  fruncount       NUMBER DEFAULT 0, --����
  fstate        NUMBER DEFAULT 0,   --״̬ 1 ����, 2����
  frunlog       VARCHAR2(200),      --�������־
  ferrorlog       VARCHAR2(200),    --�����¼
  fcomment        VARCHAR2(50),     --��ע
  fconfirm        NUMBER DEFAULT 1,   --����� 1 ,������� 0
  fcallproc       VARCHAR2(600),    --�洢���� SQL
  fcountsql       VARCHAR2(1000),    --��ѯcount SQL
  fquery          VARCHAR2(4000),   --��ѯ��ͼ SQL
  fquery2          VARCHAR2(4000),   --��ѯ��ͼ SQL2(���������ָ�)
  fcolumns        VARCHAR2(3000),    --�Զ��ŷָ������
  fcocod          VARCHAR2(4) DEFAULT 'YY' --�ϱ�Ժ������
);
-- Add comments to the columns 
comment on column TExportTASK.ftaskid
  is '������(����)';
comment on column TExportTASK.ftitle
  is '��������';
comment on column TExportTASK.frolid
  is '���������� TUPLOADROLE';
comment on column TExportTASK.fautorun
  is '�Ƿ��Զ�ִ��';
comment on column TExportTASK.fquerydate
  is '��ѯ���ڿ��:һ�Σ��ձ�,�ܱ�,�±�,����,�걨';
comment on column TExportTASK.floopday
  is '�������(��Ӧ�ձ�Ϊ1���ܱ�Ϊ���ڣ��±�Ϊ���ţ�����Ϊ��һ���ȳ����ţ��걨Ϊ��һ��1�¼���)';
comment on column TExportTASK.fexptime
  is '����ʱ��� 03:04:05';
comment on column TExportTASK.fsuffix
  is '��׺�� csv xlsx';
comment on column TExportTASK.fpath
  is '����·��';
comment on column TExportTASK.fcomment
  is '��ע��Ϣ';
comment on column TExportTASK.fconfirm
  is '��Ҫ���ȷ��';
comment on column TExportTASK.fcallproc
  is '�洢���� SQL';
comment on column TExportTASK.fcountsql
  is '��ѯcount SQL';
comment on column TExportTASK.fquery
  is '��ѯ��ͼ SQL';
comment on column TExportTASK.fcocod
  is '�ϱ�Ժ������';
-- Create/Recreate primary, unique and foreign key constraints 
alter table TExportTASK
  add constraint PKEYTASKID primary key (ftaskid);


create table TExportLOG
(
  fid             NUMBER not null,  --��ˮ��
  fdate           DATE,            --��־����ʱ��
  ftaskid         NUMBER not null, --�������ID
  ftitle          VARCHAR2(50) DEFAULT '��������',
  fresult         NUMBER DEFAULT 0,  --��� 0��ʼ״̬, 1����ִ��,2�о��浫�ܵ���,3�����޷�����,4�쳣 
  fmemo           VARCHAR2(1000),    --������ϸ��Ϣ
  fsyntax         VARCHAR2(4000),   --������﷨(���ڳ���ʱ����)
  fautorun        NUMBER DEFAULT 0, --�Զ�ִ�� 0 ��, 1�Զ�,
  fqueryStart     DATE,     --��ѯ���ڿ���滻����ʼ����
  fqueryEnd       DATE,     --��ѯ���ڿ���滻�Ľ�ֹ����
  ffilename       VARCHAR2(200),      --�����ļ���
  fsuffix         VARCHAR2(10),    --��׺�� csv xlsx
  fpath           VARCHAR2(800)      --����·��
);
alter table TExportLOG
  add constraint PKEYTASKLOGID primary key (fid);
comment on column TExportLOG.fid
  is '��ˮ��(����)';
comment on column TExportLOG.fdate
  is 'ִ��ʱ��';
comment on column TExportLOG.ftaskid
  is '������';
comment on column TExportLOG.ftitle
  is '��������';
comment on column TExportLOG.fresult
  is '��� 0��ʼ״̬,1����ִ��,2�о��浫�ܵ���,3�����޷�����,4�쳣';
comment on column TExportLOG.fmemo
  is '������ϸ��Ϣ';
comment on column TExportLOG.fsyntax
  is '������﷨(���ڳ���ʱ����)';
comment on column TExportLOG.fautorun
  is '�Ƿ��Զ�ִ��';
comment on column TExportLOG.fqueryStart
  is '�滻����ʼ����';
comment on column TExportLOG.fqueryEnd
  is '�滻�Ľ�ֹ����';
comment on column TExportLOG.fsuffix
  is '��׺�� csv xlsx';
comment on column TExportLOG.fpath
  is '����·��';
--
INSERT INTO TExportLOG(FID,FDATE,FTASKID,FTITLE,FRESULT,FMEMO,FAUTORUN,FQUERYSTART,FQUERYEND,FFILENAME,FSUFFIX,FPATH,FQUERYSTART,FQUERYEND) 
SELECT (SELECT NVL(MAX(FID)+1,1) FROM TExportLOG) FID,
SYSDATE,1942,'�ϱ�',0,'',1,SYSDATE,SYSDATE,'dbQExp','.csv','\PATH'
FROM DUAL ;

--SELECT * FROM TExportTASK;
--SELECT * FROM TExportLOG;
--SELECT NVL(MAX(ftaskid)+1,0)   FROM TExportTASK ;
--INSERT INTO TExportTASK (ftaskid,fstate,ftitle,fautorun)VALUES(1942,0,'�½��ƻ�����',0);
--	COMMIT;

--������ҳ�������ȷ�ϱ�  TMRDDE
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
  is '�ύ����';
comment on column TMRDCONFIRM.FODATE
  is '��Ժ����';
comment on column TMRDCONFIRM.FINDATE
  is '���ȷ������';
comment on column TMRDCONFIRM.FUPDATE
  is '�ϱ��ύ����';
comment on column TMRDCONFIRM.FMODIFY
  is '�޸�����';
comment on column TMRDCONFIRM.FST
  is '�ɷ��ϱ�';
comment on column TMRDCONFIRM.FUSER
  is '¼���û�';
comment on column TMRDCONFIRM.FMANAGER
  is '������';
--THQU_CHECK_RESULT
--THQU_CHECK  --VHWT_PUB002

--�ϲ�ͨ����˵�����
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

--�����ϱ���ĸ���
UPDATE TMRDCONFIRM a SET a.FUPDATE = SYSDATE, a.FMEMO = 'dbQAuto',a.FSUBMIT= a.FSUBMIT + 1
WHERE a.FODATE>=TO_DATE('2018-01-05','YYYY-MM-DD') AND a.FODATE<=TO_DATE('2018-01-10','YYYY-MM-DD');
COMMIT;

UPDATE TMRDCONFIRM a SET a.FMODIFY = add_months(SYSDATE,-17)
WHERE  a.FMEMO = 'dbQAuto' AND a.FSUBMIT>= 2;
COMMIT;
--�����޸ĵĲ���
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
  is '������(����)';
comment on column TUPLOADROLE.froletitle
  is '��������';
comment on column TUPLOADROLE.fisenable
  is '����������0/1';
comment on column TUPLOADROLE.fcomment
  is '��ע��Ϣ';
comment on column TUPLOADROLE.fcreator
  is '���򴴽���';
comment on column TUPLOADROLE.fmodifydate
  is '�޸�����';
comment on column TUPLOADROLE.fexecutecount
  is '��¼ִ�д���';
comment on column TUPLOADROLE.fstatuslog
  is '���״̬��¼';
comment on column TUPLOADROLE.frolesql
  is '����SQL�ؼ���';
comment on column TUPLOADROLE.fcocod
  is '�������õ�λcode';
comment on column TUPLOADROLE.fviewname
  is '��ͼ����';
comment on column TUPLOADROLE.froletemplateid
  is '����ģ��ID';
comment on column TUPLOADROLE.fusefortemplate
  is '��������ģ��(YΪģ��)';
comment on column TUPLOADROLE.ftemplateremark
  is '����ģ��˵��';
comment on column TUPLOADROLE.ffileheader
  is '�����ļ�ǰ׺��ʽ';
-- Create/Recreate primary, unique and foreign key constraints 
alter table TUPLOADROLE
  add constraint PKEYROLEID primary key (FROLID);
