--drop table urbanxyz_data_test.temp_table_2;
drop table urbanxyz_data_test.test_time_dmw;
--创建大麦网时间展开表（只有id和时间）
create  table urbanxyz_data_test.test_time_dmw  as
select
index,id_event
,split_part(performance_etime_final,' ',1) as date
,split_part(performance_etime_final,' ',-1) as time
,performance_stime_ori as performance_ori
from (
SELECT
distinct
 index,id_event,
performance_stime_ori,
			trim(replace(regexp_replace(performance_time_opr,'[\(（《【“].*[\)）》】”]',''),'.','-')) AS performance_stime_final,
			trim(replace(regexp_replace(performance_time_opr,'[\(（《【“].*[\)）》】”]',''),'.','-')) AS performance_etime_final
from(
select
index,id_event
,performance_stime_ori
,regexp_replace(regexp_replace(regexp_replace(performance_time_opr,'[\u4E00-\u9FA5]{1,}',' '),'[\u4E00-\u9FA5]{1,}',' '),'[\u4E00-\u9FA5]{1,}',' ') as performance_time_opr
from(
select
index,id_event
	,performance_time_opr as performance_stime_ori
,replace(replace(replace(replace(replace(replace(replace(performance_time_opr,'：',':'),'，',''),'/','-'),'+',''),'年','.'),'月','.'),'日',' ') as performance_time_opr
 from(
select
  index,id_event,performance_stime_ori,UNNEST(string_to_array(performance_stime_ori,',')) as performance_time_opr
 from urbanxyz_data_test.temp_table_1 where event_source='dmw')a)b)c)d;


--更新大麦时间表
update urbanxyz_data_test.test_time_dmw set date=null,time=null where(date,time,performance_ori) not in(
select date,time,performance_ori from urbanxyz_data_test.test_time_dmw
 where (array_length(regexp_split_to_array(date,'-'),1)-1=2 and length(date)<=10 and length(date)>=8)and (array_length(regexp_split_to_array(time,':'),1)-1<>0 and(length(time)=5 or length(time)=8)));

--大麦网原始数据关联时间表建表
CREATE TABLE urbanxyz_data_test.temp_table_2 AS
SELECT a.*, date, time, performance_ori
FROM urbanxyz_data_test.temp_table_1 a
LEFT JOIN urbanxyz_data_test.test_time_dmw b
ON a.index = b.index;
-- WHERE a.sampletime >= 'l_s' AND a.sampletime <= 'l_e';

--更新日期和时间到指定列
update urbanxyz_data_test.temp_table_2 set start_date_final=cast(date as timestamp),end_date_final=cast(date as timestamp),performance_etime_final=time,performance_stime_final=time,performance_stime_ori=performance_ori where event_source='dmw';
--删掉多余的列
ALTER TABLE urbanxyz_data_test.temp_table_2 DROP COLUMN performance_ori;
ALTER TABLE urbanxyz_data_test.temp_table_2 DROP COLUMN time;
ALTER TABLE urbanxyz_data_test.temp_table_2 DROP COLUMN date;
--更改指定列的数据类型
ALTER TABLE urbanxyz_data_test.temp_table_2 ALTER COLUMN start_date_final type date;
ALTER TABLE urbanxyz_data_test.temp_table_2 ALTER COLUMN end_date_final  type  date;
ALTER TABLE urbanxyz_data_test.temp_table_2 ALTER COLUMN performance_stime_final  TYPE time using performance_stime_final::time;
ALTER TABLE urbanxyz_data_test.temp_table_2 ALTER COLUMN performance_etime_final  TYPE time using performance_etime_final::time;
UPDATE urbanxyz_data_test.temp_table_2 a set start_date_final=b.start_date_final,end_date_final=b.end_date_final from urbanxyz_data_test.temp_table_1 b where (a.start_date_final is null) and
                                                                                                                                                        (a.id_event=b.id_event);

--测试未被展开的部分
--select * from urbanxyz_data_test.temp_table_2 where start_date_final is null
