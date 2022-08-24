# 3일차. 아파치 스파크 데이터 변환 기본

> 아파치 스파크를 통해 다양한 변환 예제를 실습합니다. 이번 장에서 사용하는 외부 오픈 포트는 4040, 4041, 8888 입니다


## 1. 최신버전 업데이트 테이블

> 원격 터미널에 접속하여 관련 코드를 최신 버전으로 내려받고, 과거에 실행된 컨테이너가 없는지 확인하고 종료합니다

### 1-1. 최신 소스를 내려 받습니다
```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training
git pull
```
<br>

### 1-2. 실습을 위한 이미지를 내려받고 컨테이너를 기동합니다
```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training/day3
docker-compose pull
docker-compose up -d
```
<br>

### 1-3. 스파크 실습을 위해 노트북 페이지에 접속합니다

> 노트북 로그를 확인하여 접속 주소와 토큰을 확인합니다

```bash
# terminal
docker-compose ps

sleep 10
docker-compose logs notebook
```
> `http://127.0.0.1:8888/?token=87e758a1fac70558a6c4b4c5dd499d420654c509654c6b01` 이러한 형식의 URL 에서 `127.0.0.1` 을 자신의 호스트 이름(`vm[number].[ssh접속한url]`)으로 변경하여 접속합니다
<br> 

### 1-4. 실습자료 git 디버깅
#### 1-4-1. 실습환경에서 perminssion 없다는 메시지가 발생
- 명령어 앞에 `sudo` 를 붙여주세요.

#### 1-4-2. git pull 을 했을 때 `fatal: cannot create directory at '파일명': Permission denied` 이 나온다면, 
- git repo 폴더의 권한을 재설정하는 방법이 있습니다.  
   
```bash
# 단, 운영환경에서 사용하는 파일에 777 권한 사용시 보안적으로 취약할 수 있습니다. 
sudo chmod -R 777 ~/home/ubuntu/work/data-engineer-intermediate-training
```

#### 1-4-3. Git - 내가 수정한 내역을 없애고 업데이트된 자료 가져오기
*주의: 내가 만든 수정 및 작업내용(notebook 실습 등) 사라집니다*  
 
```bash
 # 원하는 폴더로 이동
# cd ~/work/{폴더명}
cd ~/work/data-engineer-intermediate-training

# 내가 작업한 수정사항 없애버리기 - 로컬 repo의 이전 commit 의 파일상태로 돌아가기
git checkout -- .

# 상태를 확인해보기 - 변경사항이 없다고 보임
git status -sb

# 그동안 원격 repo-github 의 수업자료-에서 추가된 commit 내역 가져오기
git pull
```   


## 2. 데이터 변환 기본
### [1. 아파치 스파크 기본 실습](http://htmlpreview.github.io/?https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day3/notebooks/lgde-spark-core/lgde-spark-core-1-basic.html)
### [2. 아파치 스파크 연산자 실습](http://htmlpreview.github.io/?https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day3/notebooks/lgde-spark-core/lgde-spark-core-2-operators.html)
### [3. 아파치 스파크 데이터타입 실습](http://htmlpreview.github.io/?https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day3/notebooks/lgde-spark-core/lgde-spark-core-3-data-types.html)
### [4. 아파치 스파크 조인 실습](http://htmlpreview.github.io/?https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day3/notebooks/lgde-spark-core/lgde-spark-core-4-join.html)
### [5. 아파치 스파크 집계 실습](http://htmlpreview.github.io/?https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day3/notebooks/lgde-spark-core/lgde-spark-core-5-aggregation.html)
### [6. 아파치 스파크 JDBC to MySQL](http://htmlpreview.github.io/?https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day3/notebooks/lgde-spark-core/lgde-spark-core-6-jdbc-mysql.html)
### [7. 아파치 스파크 JDBC to MongoDB](http://htmlpreview.github.io/?https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day3/notebooks/lgde-spark-core/lgde-spark-core-7-jdbc-mongodb.html)

<br>

