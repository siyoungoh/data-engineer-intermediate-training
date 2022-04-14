# 5일차. 지표 생성 프로젝트

> 가상의 웹 쇼핑몰의 주요 지표를 생성하기 위한, 접속정보, 매출 및 고객정보 등의 데이터를 수집하여 기본 지표를 생성합니다


- 범례
  * :green_book: : 기본, :blue_book: : 중급

- 목차
  * [1. 최신버전 업데이트](#1-최신버전-업데이트)
  * [2. 노트북 컨테이너 기동](#2-green_book-노트북-컨테이너-기동)
  * [3. Spark 자료 확인](#3-green_book-수집된-데이터-탐색)
  * [4. 기본 지표 생성](#4-green_book-기본-지표-생성)
  * [5. 고급 지표 생성](#5-blue_book-고급-지표-생성)
  * [6. 컨테이너 종료](#6-green_book-질문-및-컨테이너-종료)

- 실습 노트북
  * [실습 노트북](https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day5/notebooks/data-engineer-training-course.ipynb)
  * [정답 노트북](https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day5/notebooks/data-engineer-training-course-answer.ipynb)

<br>

## 1. 최신버전 업데이트
> 원격 터미널에 접속하여 관련 코드를 최신 버전으로 내려받고, 과거에 실행된 컨테이너가 없는지 확인하고 종료합니다

### 1-1. 최신 소스를 내려 받습니다
```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training
git pull
```

### 1-2. 현재 기동되어 있는 도커 컨테이너를 확인하고, 종료합니다

#### 1-2-1. 현재 기동된 컨테이너를 확인합니다
```bash
# terminal
docker ps -a
```

#### 1-2-2. 기동된 컨테이너가 있다면 강제 종료합니다
```bash
# terminal 
docker rm -f `docker ps -aq`
```
> 다시 `docker ps -a` 명령으로 결과가 없다면 모든 컨테이너가 종료되었다고 보시면 됩니다
<br>

[목차로 돌아가기](#5일차-데이터-엔지니어링-프로젝트)
<br>
<br>


## 2. :green_book: 노트북 컨테이너 기동

> 본 장에서 수집한 데이터를 활용하여 데이터 변환 및 지표 생성작업을 위하여 주피터 노트북을 열어둡니다

### 2-1. 노트북 주소를 확인하고, 크롬 브라우저로 접속합니다

#### 2-1-1. 노트북 기동 및 확인

* 노트북을 기동합니다
```bash
# terminal
cd /home/ubuntu/work/data-engineer-intermediate-training/day5
docker-compose pull
docker-compose up -d
```

* 기동된 노트북의 접속정보를 확인하고 접속합니다
```bash
# terminal
docker-compose logs notebook | grep 8888
```
> 출력된  URL을 복사하여 `127.0.0.1:8888` 대신 개인 `<hostname>.aiffelbiz.co.kr:8888` 으로 변경하여 크롬 브라우저를 통해 접속하면, jupyter notebook lab 이 열리고 work 폴더가 보이면 정상기동 된 것입니다

#### 2-1-2. 기 생성된 실습용 노트북을 엽니다
* 좌측 메뉴에서 "data-engineer-lgde-day5.ipynb" 을 더블클릭합니다

#### 2-1-3. 신규로 노트북을 만들고 싶은 경우
* `Launcher` 탭에서 `Notebook - Python 3` 를 선택하고
* 탭에서 `Rename Notebook...` 메뉴를 통해 이름을 변경할 수 있습니다

<br>

[목차로 돌아가기](#5일차-데이터-엔지니어링-프로젝트)
<br>
<br>


## 3. 스파크 실습 자료
- [3일차. 아파치 스파크](https://github.com/siyoungoh/data-engineer-intermediate-training/tree/master/day3/README.md)

## 4. 시나리오와 주요 지표 소개

## 5. 노트북 실습
- 실습 노트북
  * [실습 노트북](https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day5/notebooks/data-engineer-training-course.ipynb)
  * [정답 노트북](https://github.com/siyoungoh/data-engineer-intermediate-training/blob/master/day5/notebooks/data-engineer-training-course-answer.ipynb)


## 6. :green_book: 컨테이너 종료

### 6-2. 컨테이너 종료

```python
docker-compose down
```

> 아래와 같은 메시지가 출력되고 모든 컨테이너가 종료되면 정상입니다
```text
⠿ Container notebook  Removed        3.7s
```
<br>

