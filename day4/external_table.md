# 하이브 외부 저장소 테이블

#### Table 의 종류
[Hive wiki - Managed vs. External Tables](https://cwiki.apache.org/confluence/display/Hive/Managed+vs.+External+Tables)

- Managed Table : 옵션을 적지 않으면 기본적으로 managed table 이 생성됨. 세션이 종료되어도 테이블의 데이터와 파일은 유지됨. 
**테이블을 Drop 하면 파일도 함께 삭제됨.**
- External Table : 테이블을 Drop 해도 파일은 그대로 유지됨. 기존에 있는 데이터에 대해 table 을 만들어 관리하기도 함. link 한다고 표현. 사용자가 실수로 table 을 drop 해도 데이터는 유지되므로 external table 로 관리를 하는 것이 유리할 수도 있음. 
- Temporary Table : 현재 hive 세션동안에만 유지되는 임시 테이블. 
[Create a temporary table by Cloudera](https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.0.1/using-hiveql/content/hive_create_a_hive_temporary_table.html)  

  
> 하이브의 경우 local 데이터를 하둡에 load 하여 Managed 테이블을 생성할 수도 있지만, 대게 외부 데이터 수집 및 적재의 경우 External 테이블로 생성합니다  
  
### 1. 매출 테이블의 외부 제공을 위해 외부 테이블로 생성합니다

> 로컬 경로에 테이블 parquet 파일이 존재하므로, 해당 파일을 이용하여 생성합니다


```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training/day4
git pull
```

* 하이브 컨테이너로 접속합니다
```bash
# terminal
docker-compose exec hive-server bash
```
<br>

* 원본 파일의 스키마를 확인 및 파일을 하둡 클러스터에 업로드합니다
```
hadoop jar /tmp/source/parquet-tools-1.8.1.jar schema file:///tmp/source/purchase/20201025/38dc1f5b-d49d-436d-a84a-4e5c2a4022a5.parquet
```
```text
message purchase_20201025 {
  optional binary p_time (UTF8);
  optional int32 p_uid;
  optional int32 p_id;
  optional binary p_name (UTF8);
  optional int32 p_amount;
}
```
<br>

* 경로 확인 및 생성
```bash
hadoop fs -mkdir -p /user/lgde/purchase/dt=20201025
hadoop fs -mkdir -p /user/lgde/purchase/dt=20201026
```
```sql
hadoop fs -put /tmp/source/purchase/20201025/* /user/lgde/purchase/dt=20201025
hadoop fs -put /tmp/source/purchase/20201026/* /user/lgde/purchase/dt=20201026
```

* 하이브 명령 수행을 위해 beeline 을 실행합니다
```bash
beeline
```
* 콘솔로 접속하여 데이터베이스 및 테이블을 생성합니다 
```bash
# beeline>
!connect jdbc:hive2://localhost:10000 scott tiger
```
```sql
# beeline>
create database if not exists testdb comment 'test database' 
  location '/user/lgde/warehouse/testdb'
  with dbproperties ('createdBy' = 'lgde');
```
```sql
use testdb;

create external table if not exists purchase (
  p_time string
  , p_uid int
  , p_id int
  , p_name string
  , p_amount int
) partitioned by (dt string) 
row format delimited 
stored as parquet 
location 'hdfs:///user/lgde/purchase';
```
```sql
alter table purchase add if not exists partition (dt = '20201025') location 'hdfs:///user/lgde/purchase/dt=20201025';
alter table purchase add if not exists partition (dt = '20201026') location 'hdfs:///user/lgde/purchase/dt=20201026';
```
<br>


* 생성된 하이브 테이블을 조회합니다
```sql
# beeline>
show partitions purchase;
select * from purchase where dt = '20201025';
```
<br>

* 일자별 빈도를 조회합니다
```sql
# beeline>
select dt, count(1) as cnt from purchase group by dt;
```

### 2. 고객 테이블의 외부 제공을 위해 외부 테이블로 생성합니다

> 마찬가지로 유사한 방식으로 적재 및 테이블 생성을 수행합니다

* 하이브 컨테이너로 접속합니다
```bash
# terminal
docker-compose exec hive-server bash
```

* 파일 업로드 및 스키마 확인, 경로 생성 및 업로드

```bash
# docker
hadoop fs -mkdir -p /user/lgde/user/dt=20201025
hadoop fs -mkdir -p /user/lgde/user/dt=20201026
```
```sql
hadoop fs -put /tmp/source/user/20201025/* /user/lgde/user/dt=20201025
hadoop fs -put /tmp/source/user/20201026/* /user/lgde/user/dt=20201026
```
```sql
hadoop jar /tmp/source/parquet-tools-1.8.1.jar schema file:///tmp/source/user/20201025/2e3738ff-5e2b-4bec-bdf4-278fe21daa3b.parquet
```
```text
message user_20201025 {
  optional int32 u_id;
  optional binary u_name (UTF8);
  optional binary u_gender (UTF8);
  optional int32 u_signup;
}
```
<br>


* 하이브 명령 수행을 위해 beeline 을 실행합니다
```bash
beeline
```
* 하이브 테이블 생성 및 조회
```bash
# beeline>
!connect jdbc:hive2://localhost:10000 scott tiger
```
```sql
# beeline>
drop table if exists `user`;

create external table if not exists `user` (
  u_id int
  , u_name string
  , u_gender string
  , u_signup int
) partitioned by (dt string)
row format delimited 
stored as parquet 
location 'hdfs:///user/lgde/user';
```
```sql
alter table `user` add if not exists partition (dt = '20201025') location 'hdfs:///user/lgde/user/dt=20201025';
alter table `user` add if not exists partition (dt = '20201026') location 'hdfs:///user/lgde/user/dt=20201026';
```
<br>

* 생성된 결과를 확인합니다
```sql
# beeline>
select * from `user` where dt = '20201025';
```
```sql
select dt, count(1) as cnt from `user` group by dt;
```
<br>