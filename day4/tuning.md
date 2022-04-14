# 하이브 트러블 슈팅 및 성능 개선
- 목차

* [3. 하이브 트러블슈팅 가이드](#3-하이브-트러블슈팅-가이드)
    - [3-1. 파티셔닝을 통한 성능 개선](#3-1-파티셔닝을-통한-성능-개선)
    - [3-2. 파일포맷 변경을 통한 성능 개선](#3-2-파일포맷-변경을-통한-성능-개선)
    - [3-3. 비정규화를 통한 성능 개선](#3-3-비정규화를-통한-성능-개선)
    - [3-4. 글로벌 정렬 회피를 통한 성능 개선](#3-4-글로벌-정렬-회피를-통한-성능-개선)
    - [3-5. 버킷팅을 통한 성능 개선](#3-5-버킷팅을-통한-성능-개선)


## 1. 최신버전 업데이트
> 원격 터미널에 접속하여 관련 코드를 최신 버전으로 내려받고, 과거에 실행된 컨테이너가 없는지 확인하고 종료합니다

### 1-1. 최신 소스를 내려 받습니다
```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training/day4
git pull
```
<br>

### 1-2. 현재 기동되어 있는 도커 컨테이너를 확인하고, 종료합니다

#### 1-2-1. 현재 기동된 컨테이너를 확인합니다
```bash
# terminal
docker ps -a
```
<br>


#### 1-2-2. 기동된 컨테이너가 있다면 강제 종료합니다
```bash
# terminal 
docker rm -f `docker ps -aq`
```
> 다시 `docker ps -a` 명령으로 결과가 없다면 모든 컨테이너가 종료되었다고 보시면 됩니다
<br>


#### 1-2-3. 하이브 실습을 위한 컨테이너를 기동합니다
```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training/day4
docker-compose pull
docker-compose up -d
docker-compose ps
```
<br>


#### 1-2-4. 실습에 필요한 IMDB 데이터를 컨테이너로 복사합니다
```bash
# terminal
docker cp data/imdb.tsv hive-server:/opt/hive/examples/imdb.tsv
docker-compose exec hive-server ls /opt/hive/examples
```

> 마지막 ls /opt/hive/examples 명령어 결과로 imdb.tsv 파일이 확인되면 정상입니다
<br>


#### 1-2-5. 하이브 컨테이너로 접속합니다
```bash
# terminal
echo "하이브 서버가 기동 되는데에 시간이 좀 걸립니다... 30초 후에 접속합니다"
sleep 30 
docker-compose exec hive-server bash
```
<br>


#### 1-2-6. beeline 프로그램을 통해 hive 서버로 접속합니다

> 여기서 `beeline` 은 Hive (벌집)에 접속하여 SQL 명령을 수행하기 위한 커맨드라인 프로그램이며, Oracle 의 SQL\*Plus 와 같은 도구라고 보시면 됩니다

* 도커 컨테이너에서 beeline 명령을 수행하면 프롬프트가 `beeline>` 으로 변경되고, SQL 명령의 수행이 가능합니다
```bash
# docker
beeline
```
<br>


* beeline 프롬프트가 뜨면 Hive Server 에 접속하기 위해 대상 서버로 connect 명령을 수행합니다
```bash
# beeline
!connect jdbc:hive2://localhost:10000 scott tiger
```
> 아래와 같은 메시지가 뜨면 성공입니다

```bash
Connecting to jdbc:hive2://localhost:10000
Connected to: Apache Hive (version 2.3.2)
Driver: Hive JDBC (version 2.3.2)
Transaction isolation: TRANSACTION_REPEATABLE_READ
```

## 2 하이브의 파티션과 버킷
![hive data model](https://firebasestorage.googleapis.com/v0/b/firescript-577a2.appspot.com/o/imgs%2Fapp%2Fsyoh%2FBPFjXgR8_B.png?alt=media&token=e46ca499-43a4-4920-94f8-412ddf75be98)

- 파티션
  - 하이브의 쿼리는 모든 하이브 테이블을 스캔하는 것이 기본값. 
  - 데이터가 많은 테이블을 쿼리하면 성능저하가 발생함. 
  - 각 파티션은 hdfs 테이블의 디렉토리의 하위 디렉토리에 저장함. 
  - 테이블이 쿼리를 받으면 테이블 데이터 중 필요한 파티션(디렉토리)만 쿼리되기 때문에 I/O 와 쿼리시간을 많이 줄일 수 있음. 테이블 만들 때 구현하는게 간단하고 생성 파티션 확인하기도 쉬움. 

- 버킷 
  - 쿼리 성능을 최적화하기 위해 데이터 집합을 관리할 수 있는 작은 부분으로 잘게 나누는 또 다른 기술. HDFS 의 파일 세그먼트. 
  - 버킷 개수 : 각 버킷에 너무 많거나 적은 데이터를 사용하지 않아야함. 좋은 선택은 HDFS 두 블록 정도. 만약 하둡 블록크기가 256mb 라면 각 버킷에 512mb 를 계획. 2^n 을 버킷 개수로 사용하는 것을 추천. 



## 3 하이브 트러블슈팅 가이드

> IMDB 영화 예제를 통해 테이블을 생성하고, 다양한 성능 개선 방법을 시도해보면서 왜 그리고 얼마나 성능에 영향을 미치는 지 파악합니다


### 3-1 파티셔닝을 통한 성능 개선

#### 3-1.1. 하이브 서버로 접속합니다

* 하이브 터미널을 통해 JDBC Client 로 하이브 서버에 접속합니다
  - 이미 접속된 세션이 있다면 그대로 사용하셔도 됩니다
```bash
# terminal
docker-compose exec hive-server bash
```
* Beeline 통해서 하이브 서버로 접속 후, testdb 를 이용합니다
```sql
# beeline> 
!connect jdbc:hive2://localhost:10000 scott tiger
use testdb;
```
<br>


#### 3-1.2. 데이터집합의 스키마를 확인하고 하이브 테이블을 생성합니다
* 데이터집합은 10년(2006 ~ 2016)의 가장 인기있는 1,000개의 영화에 대한 데이터셋입니다

| 필드명 | 설명 |
| --- | --- |
| Title | 제목 |
| Genre | 장르 |
| Description | 설명 |
| Director | 감독 |
| Actors | 배우 |
| Year | 년도 |
| Runtime | 상영시간 |
| Rating | 등급 |
| Votes | 투표 |
| Revenue | 매출 |
| Metascrore | 메타스코어 |

<br>

* 실습을 위해 `imdb_movies` 테이블을 생성합니다
```sql
# beeline> 
drop table if exists imdb_movies;

create table imdb_movies (
  rank int
  , title string
  , genre string
  , description string
  , director string
  , actors string
  , year string
  , runtime int
  , rating string
  , votes int
  , revenue string
  , metascore int
) row format delimited fields terminated by '\t';

load data local inpath '/opt/hive/examples/imdb.tsv' into table imdb_movies;
```

<details><summary> :blue_book: 12. [중급] 년도(year) 별 개봉된 영화의 수를 년도 오름차순(asc)으로 출력하세요 </summary>

```sql
select year, count(title) as movie_count from imdb_movies group by year order by year asc;
```
* 아래와 유사하게 나오면 정답입니다
```text
+-------+--------------+
| year  | movie_count  |
+-------+--------------+
| 2006  | 44           |
| 2007  | 53           |
| 2008  | 52           |
| 2009  | 51           |
| 2010  | 60           |
| 2011  | 63           |
| 2012  | 64           |
| 2013  | 91           |
| 2014  | 98           |
| 2015  | 127          |
| 2016  | 297          |
+-------+--------------+
```

</details>
<br>

* 문자열을 숫자로 캐스팅 하기위한 함수
  - `imdb_movies` 의 `revenue` 컬럼은 문자열이므로 float, double 로 형변환이 필요합니다
  - cast ( column as type ) as `new_column` 

<details><summary> :blue_book: 13. [중급] 2015년도 개봉된 영화 중에서 최고 매출 Top 3 영화 제목과 매출금액을 출력하세요 </summary>

```sql
select title, cast(revenue as float) as rev from imdb_movies where year = '2015' order by rev desc limit 3;
```
* 아래와 유사하게 나오면 정답입니다
```text
+---------------------------------------------+---------+
|                    title                    |   rev   |
+---------------------------------------------+---------+
| Star Wars: Episode VII - The Force Awakens  | 936.63  |
| Jurassic World                              | 652.18  |
| Avengers: Age of Ultron                     | 458.99  |
+---------------------------------------------+---------+
```

</details>
<br>


#### 3-1.3. 파티션 구성된 테이블을 생성합니다

> 파티션이란 데이터의 특정 컬럼 혹은 값을 기준으로, 물리적으로 다른 경로에 저장하는 기법을 말합니다. 즉, 년도 별로 다른 경로에 저장하거나, 특정 카테고리 값(동물,식물 등)으로 구분하여 저장하여 성능을 높이는 기법입니다. **주로 자주 조회되는 컬럼을 기준으로 파티셔닝 하는 것이 가장 효과적입니다** 


* 가장 일반적으로 많이 사용하는 년도를 파티션 값으로 지정하여 따로 저장합니다
  - [다이나믹 파티션은 마지막 SELECT 절 컬럼을 사용합니다](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-DynamicPartitionInserts)
  - partitioned by (year string) : 경로별로 나뉘어 저장한다는 것을 선언
  - dynamic.partition=true : 데이터 값에 따라 동적으로 저장함을 지정
  - dynamic.partition.mode=nonsstrict : strict 로 지정하는 경우 실수로 overwrite 를 막기 위해 저장시에 반드시 1개 이상의 파티션을 명시해야만 수행되는 엄격한 옵션
<br>

```sql
# beeline> 
drop table if exists imdb_partitioned;

create table imdb_partitioned (
  rank int
  , title string
  , genre string
  , description string
  , director string
  , actors string
  , runtime int
  , rating string
  , votes int
  , revenue string
  , metascore int
) partitioned by (year string) row format delimited fields terminated by '\t';

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
```
<br>

* 기존 테이블로부터 데이터를 가져와서 동적으로 파티셔닝하여 저장합니다
```sql
insert overwrite table imdb_partitioned partition (year) 
  select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore, year 
  from imdb_movies;
```
* 신규로 생성된 파티션 테이블을 통해 집계 연산을 수행합니다
```sql
select year, count(1) as cnt from imdb_partitioned group by year;
```
<br>

* Explain 명령을 통해서 2가지 테이블 조회 시의 차이점을 비교합니다
  - explain 명령은 실제 조회작업을 수행하지 않고 어떻게 수행할 계획을 출력하는 옵션입니다
```sql
# beeline> 
explain select year, count(1) as cnt from imdb_movies group by year;
explain select year, count(1) as cnt from imdb_partitioned group by year;
```
<br>

* **일반 테이블과, 파티셔닝 테이블의 성능을 비교합니다**
```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training/day4/ex1
vimdiff agg.imdb_movies.out agg.imdb_partitioned.out
```
* 관련 링크
  * [Hive Language Manul DML](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML#LanguageManualDML-DynamicPartitionInserts)

[목차로 돌아가기](#4일차-아파치-하이브-데이터-적재)

<br>
<br>


### 3-2 파일포맷 변경을 통한 성능 개선

> 파일포맷을 텍스트 대신 파케이 포맷으로 변경하는 방법을 익히고, 예상한 대로 결과가 나오는지 확인합니다 

#### 3-2-1. 파케이 포맷과 텍스트 파일포맷의 성능차이 비교

* 파케이 포맷 기반의 테이블을 CTAS (Create Table As Select) 통해 생성합니다 (단, CTAS 는 SELECT 절을 명시하지 않습니다)
```sql
# beeline>
drop table if exists imdb_parquet;

create table imdb_parquet row format delimited stored
    as parquet as select * 
    from imdb_movies;

select year, count(1) as cnt from imdb_parquet group by year;
```
<br>


* **파티셔닝 테이블과 파케이 포맷 테이블의 성능을 비교합니다**
```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training/day4/ex1
vimdiff agg.imdb_partitioned.out agg.imdb_parquet.out
```
<br>


#### 3-2-2. 텍스트, 파티션 및 파케이 테이블의 성능차이 비교
```sql
# beeline>
explain select year, count(1) as cnt from imdb_movies group by year;
# Statistics: Num rows: 3096 Data size: 309656 Basic stats: COMPLETE Column stats: NONE

explain select year, count(1) as cnt from imdb_partitioned group by year;
# Statistics: Num rows: 1000 Data size: 302786 Basic stats: COMPLETE Column stats: NONE

explain select year, count(1) as cnt from imdb_parquet group by year;
#  Statistics: Num rows: 1000 Data size: 12000 Basic stats: COMPLETE Column stats: NONE
```
<br>


#### 3-2-3. 필요한 컬럼만 유지하는 것도 성능에 도움이 되는지 비교
* 파케이 포맷의 정렬된 테이블을 생성합니다
```sql
# beeline>
create table imdb_parquet_small stored as parquet 
  as select title, rank, metascore, year 
  from imdb_movies sort by metascore;

explain select rank, title, metascore from imdb_parquet order by metascore desc limit 10;
# Statistics: Num rows: 1000 Data size: 12000 Basic stats: COMPLETE Column stats: NONE

explain select rank, title, metascore from imdb_parquet_sorted order by metascore desc limit 10;
# Statistics: Num rows: 1000 Data size: 4000 Basic stats: COMPLETE Column stats: NONE

explain select rank, title, metascore from imdb_parquet_small order by metascore desc limit 10;
# Statistics: Num rows: 1000 Data size: 4000 Basic stats: COMPLETE Column stats: NONE
```
<br>


* **필요한 컬럼만 유지하는 경우에도 성능개선의 효과가 있는지 비교합니다**
```bash
cd /home/ubuntu/work/data-engineer-intermediate-training/day4/ex2
vimdiff sort.imdb_parquet.out sort.imdb_parquet_small.out
```

[목차로 돌아가기](#4일차-아파치-하이브-데이터-적재)

<br>
<br>


### 3-3. 비정규화를 통한 성능 개선

> 일반적으로 관계형 데이터베이스의 경우 Redundent 한 데이터 적재를 피해야만 Consistency 문제를 회피할 수 있고 변경 시에 일관된 데이터를 저장할 수 있습니다. 그래서 **PK, FK 등으로 Normalization & Denormalization 과정**을 거치면서 모델링을 하게 됩니다. 하지만 **분산 환경에서의 정규화 했을 때의 관리 비용 보다 Join 에 의한 리소스 비용이 더 큰** 경우가 많고 Join 의 문제는 Columnar Storage 나 Spark 의 도움으로 많은 부분 해소될 수 있기 때문에 ***Denormalization 을 통해 Superset 데이터를 가지는 경우***가 더 많습니다. 

> Daily Batch 작업에서 아주 큰 Dimension 데이터를 생성하고 Daily Logs 와 Join 한 모든 데이터를 가진 Fact 데이터를 생성 (User + Daily logs) 하고 이 데이터를 바탕으로 일 별 Summary 혹은 다양한 분석 지표를 생성하는 것이 효과적인 경우가 많습니다

#### 3-4-1. 분산환경의 모델링 방향
* 트랜잭션 및 Consistency 성능을 희생하여 처리량과 조회 레이턴시를 향상
* OLAP 성 데이터 분석 조회는 Join 대신 모든 필드를 하나의 테이블에 다 가진 Superset 테이블이 더 효과적입니다

![star-schema](images/star-schema.jpg)

[목차로 돌아가기](#4일차-아파치-하이브-데이터-적재)

<br>
<br>


### 3-4. 글로벌 정렬 회피를 통한 성능 개선

>  Order By, Group By, Distribute By, Sort By, Cluster By 실습을 통해 차이점을 이해하고 활용합니다


#### 3-4-1. 실습을 위한 employee 및 department 테이블을 생성합니다

* 컨테이너에 접속된 세션이 없다면 하이브 서버에 접속합니다
```bash
# terminal
docker-compose exec hive-server bash
```
* 중복제거 하여 `emp.uniq.txt` 파일을 생성합니다
```bash
# docker
cd /opt/hive/examples/files
cat emp.txt | sort | uniq > emp.uniq.txt
cat emp.uniq.txt
```
<br>

* 최종 결과 파일은 다음과 같습니다
```sql
/**
John|31|6
Jones|33|2
Rafferty|31|1
Robinson|34|4
Smith|34|5
Steinberg|33|3
*/
```

* Beeline 커맨드를 통해 직원 및 부서 테이블을 생성합니다
```bash
# terminal
beeline jdbc:hive2://localhost:10000 scott tiger
use testdb;
```
<br>


* 직원(employee) 테이블을 생성합니다, 데이터를 로딩합니다
```sql
# beeline>
drop table if exists employee;

create table employee (
  name string
  , dept_id int
  , seq int
) row format delimited fields terminated by '|';
```
```sql
load data local inpath '/opt/hive/examples/files/emp.uniq.txt' into table employee;
```
<br>


* 부서 테이블 정보
  - 테이블 이름 : department
  - 테이블 파일 : /opt/hive/examples/files/dept.txt
  - 테이블 스키마 : (id int, name string) 

<details><summary> :blue_book: 14. [중급] 유사한 방식으로  부서(department) 테이블을 생성하고 데이터를 로딩하세요 </summary>

```sql
drop table if exists department;

create table department (
  id int
  , name string
) row format delimited fields terminated by '|';

load data local inpath '/opt/hive/examples/files/dept.txt' into table department;
```

</details>
<br>


* 테이블 스키마 정보와 데이터를 눈으로 확인합니다
```
# beeline>
desc employee;
select * from employee;

desc department;
select * from department;
```  

<details><summary> :blue_book: 15. [중급] employee + department 정보를 가진 테이블을 조회하는 SQL문을 수행하세요 </summary>

```sql
# beeline
select e.dept_id, e.name, e.seq, d.id, d.name from employee e join department d on e.dept_id = d.id;
```
> 아래와 유사하게 나오면 정답입니다
```bash
+------------+------------+--------+-------+--------------+
| e.dept_id  |   e.name   | e.seq  | d.id  |    d.name    |
+------------+------------+--------+-------+--------------+
| 31         | John       | 6      | 31    | sales        |
| 33         | Jones      | 2      | 33    | engineering  |
| 31         | Rafferty   | 1      | 31    | sales        |
| 34         | Robinson   | 4      | 34    | clerical     |
| 34         | Smith      | 5      | 34    | clerical     |
| 33         | Steinberg  | 3      | 33    | engineering  |
+------------+------------+--------+-------+--------------+
```

</details>
<br>


* `직원 테이블 (emp_dept)` 생성
  - 테이블 이름 : `emp_dept`
  - 테이블 스키마 : (id int, seq int, name string, dept name) 

<details><summary> :blue_book: 16. [중급] CTAS 구문을 이용하여 아이디(id), 순번(seq), 이름(name), 부서(dept) 를 가진 테이블을 생성하세요 </summary>

```sql
create table emp_dept as select e.dept_id as dept_id, e.seq as seq, e.name as name, d.name as dept 
    from employee e join department d on e.dept_id = d.id;

desc emp_dept;
```
* 아래와 유사하게 나오면 정답입니다
```bash
+-----------+------------+----------+
| col_name  | data_type  | comment  |
+-----------+------------+----------+
| dept_id   | int        |          |
| seq       | int        |          |
| name      | string     |          |
| dept      | string     |          |
+-----------+------------+----------+
```

</details>
<br>


#### 3-4-2. Order By
```sql
# beeline>
select * from employee order by dept_id;
```
##### Order By : 모든 데이터가 해당 키에 대해 정렬됨을 보장합니다
```bash
+----------------+-------------------+---------------+
| employee.name  | employee.dept_id  | employee.seq  |
+----------------+-------------------+---------------+
| Rafferty       | 31                | 1             |
| John           | 31                | 6             |
| Steinberg      | 33                | 3             |
| Jones          | 33                | 2             |
| Smith          | 34                | 5             |
| Robinson       | 34                | 4             |
+----------------+-------------------+---------------+
```
<br>


#### 3-4-3. Group By
```sql
# beeline>
select dept_id, count(*) from employee group by dept_id;
```
##### Group By : 군집 후 집계함수를 사용할 수 있습니다
```bash
+----------+------+
| dept_id  | _c1  |
+----------+------+
| 31       | 2    |
| 33       | 2    |
| 34       | 2    |
+----------+------+
```
<br>


#### 3-4-4. Sort By
```sql
# beeline>
set mapred.reduce.task = 2;
select * from employee sort by dept_id desc;
```
##### Sort By : 해당 파티션 내에서만 정렬을 보장합니다
* mapred.reduce.task = 2 라면 2개의 개별 파티션 내에서만 정렬됩니다
```bash
+----------------+-------------------+---------------+
| employee.name  | employee.dept_id  | employee.seq  |
+----------------+-------------------+---------------+
| Smith          | 34                | 5             |
| Robinson       | 34                | 4             |
| Steinberg      | 33                | 3             |
| Jones          | 33                | 2             |
| Rafferty       | 31                | 1             |
| John           | 31                | 6             |
+----------------+-------------------+---------------+
```
<br>


#### 3-4-5. Distribute By
```sql
# beeline>
select * from employee distribute by dept_id;
```
##### Distribute By : 단순히 해당 파티션 별로 구분되어 실행됨을 보장합니다 - 정렬을 보장하지 않습니다.
```bash
+----------------+-------------------+---------------+
| employee.name  | employee.dept_id  | employee.seq  |
+----------------+-------------------+---------------+
| Steinberg      | 33                | 3             |
| Smith          | 34                | 5             |
| Robinson       | 34                | 4             |
| Rafferty       | 31                | 1             |
| Jones          | 33                | 2             |
| John           | 31                | 6             |
+----------------+-------------------+---------------+
```
<br>


#### 3-4-6. Distribute By + Sort By
```sql
# beeline>
select * from employee distribute by dept_id sort by dept_id asc, seq desc;
```
##### Distribute By Sort By : 파티션과 해당 필드에 대해 모두 정렬을 보장합니다
```bash
+----------------+-------------------+---------------+
| employee.name  | employee.dept_id  | employee.seq  |
+----------------+-------------------+---------------+
| John           | 31                | 6             |
| Rafferty       | 31                | 1             |
| Steinberg      | 33                | 3             |
| Jones          | 33                | 2             |
| Smith          | 34                | 5             |
| Robinson       | 34                | 4             |
+----------------+-------------------+---------------+
```

#### 3-4-7. Cluster By
```sql
# beeline>
select * from employee cluster by dept_id;
```
##### Cluster By : 파티션 정렬만 보장합니다 - 특정필드의 정렬이 필요하면 Distribute By Sort By 를 사용해야 합니다
```bash
+----------------+-------------------+---------------+
| employee.name  | employee.dept_id  | employee.seq  |
+----------------+-------------------+---------------+
| Rafferty       | 31                | 1             |
| John           | 31                | 6             |
| Steinberg      | 33                | 3             |
| Jones          | 33                | 2             |
| Smith          | 34                | 5             |
| Robinson       | 34                | 4             |
+----------------+-------------------+---------------+
```
<br>


#### 3-4-7. Global Top One

> 레코드 수가 하나의 장비에서 정렬하기 어려울 만큼 충분히 많은 경우 Global Order 대신 어떤 방법을 쓸 수 있을까?

* Differences between rank and `row_number` : rank 는 tiebreak 시에 같은 등수를 매기고 다음 등수가 없으나 `row_number` 는 아님
```sql
# beeline>
select * from (
    select name, dept_id, seq, rank() over (
        partition by dept_id order by seq desc
    ) as rank from employee 
) t where rank < 2;
```

* 클러스터 별 상위 1명을 출력합니다
```
+------------+------------+--------+---------+
|   t.name   | t.dept_id  | t.seq  | t.rank  |
+------------+------------+--------+---------+
| John       | 31         | 6      | 1       |
| Steinberg  | 33         | 3      | 1       |
| Smith      | 34         | 5      | 1       |
+------------+------------+--------+---------+
```

[목차로 돌아가기](#4일차-아파치-하이브-데이터-적재)

<br>
<br>


### 3-5 버킷팅을 통한 성능 개선

> 버킷팅을 통해 생성된 테이블의 조회 성능이 일반 파케이 테이블과 얼마나 성능에 차이가 나는지 비교해봅니다


#### 3-5-1. 파티션과 버킷을 모두 사용하여 테이블을 생성합니다

* 파케이 테이블의 생성시에 버킷을 통한 인덱스를 추가해서 성능을 비교해 봅니다
  -  단, CTAS 에서는 partition 혹은 clustered 를 지원하지 않습니다
```sql
# beeline>
create table imdb_parquet_bucketed (
    rank int
  , title string
  , genre string
  , description string
  , director string
  , actors string
  , runtime int
  , rating string
  , votes int
  , revenue string
  , metascore int
) partitioned by (year string) 
clustered by (rank) sorted by (metascore) into 10 buckets 
row format delimited 
fields terminated by ',' stored as parquet;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
```
<br>


* 프로그래밍을 하면 더  좋지만, 현재는 수작업으로 직접 넣어주겠습니다 
```
insert overwrite table imdb_parquet_bucketed partition(year='2006') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2006';

insert overwrite table imdb_parquet_bucketed partition(year='2007') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2007';

insert overwrite table imdb_parquet_bucketed partition(year='2008') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2008';

insert overwrite table imdb_parquet_bucketed partition(year='2009') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2009';

insert overwrite table imdb_parquet_bucketed partition(year='2010') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2010';

insert overwrite table imdb_parquet_bucketed partition(year='2011') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2011';

insert overwrite table imdb_parquet_bucketed partition(year='2012') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2012';

insert overwrite table imdb_parquet_bucketed partition(year='2013') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2013';

insert overwrite table imdb_parquet_bucketed partition(year='2014') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2014';

insert overwrite table imdb_parquet_bucketed partition(year='2015') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2015';

insert overwrite table imdb_parquet_bucketed partition(year='2016') 
    select rank, title, genre, description, director, actors, runtime, rating, votes, revenue, metascore 
    from imdb_movies where year = '2016';
```
<br>


* 생성된 파케이 테이블이 정상적으로 버킷이 생성되었는지 확인합니다
```sql
# beeline>
desc formatted imdb_parquet_bucketed;
```
* 버킷 컬럼과, 정렬 컬럼을 확인합니다
```bash
...
| Num Buckets:                  | 10                                                 | NULL                        |
| Bucket Columns:               | [rank]                                             | NULL                        |
| Sort Columns:                 | [Order(col:metascore, order:1)]                    | NULL                        |
...
```

* 일반 파케이 테이블과 버킷이 생성된 테이블의 스캔 성능을 비교해봅니다

> TableScan 텍스트 대비 레코드 수에서는 2% (44/1488 Rows)만 읽어오며, 데이터 크기 수준에서는 약 0.1% (484/309,656 Bytes)만 읽어오는 것으로 성능 향상이 있습니다

```sql
# beeline>
explain select rank, metascore, title from imdb_movies where year = '2006' and rank < 101 order by metascore desc;
# Statistics: Num rows: 1488 Data size: 309656 Basic stats: COMPLETE Column stats: NONE

explain select rank, metascore, title from imdb_parquet where year = '2006' and rank < 101 order by metascore desc;
# Statistics: Num rows: 1000 Data size: 12000 Basic stats: COMPLETE Column stats: NONE

explain select rank, metascore, title from imdb_parquet_bucketed where year = '2006' and rank < 101 order by metascore desc;
# Statistics: Num rows: 44 Data size: 484 Basic stats: COMPLETE Column stats: NONE
```

[목차로 돌아가기](#4일차-아파치-하이브-데이터-적재)

<br>