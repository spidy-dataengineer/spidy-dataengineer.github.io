# 경력기술서 아카이브 (비공개)

> 배포되지 않는 문서다. `_config.yml`의 `exclude`에 걸려 있어 git에는 저장되지만 사이트에서는 접근할 수 없다.
>
> **목적** — 나중에 기억이 흐려졌을 때 되짚어볼 세부 사항을 모아둔다.
> 공개본(`korean/details/index.html`)에는 판단과 결과만 남기고, 설정값·명령어·코드 수준의 세부는 여기에 둔다.
>
> **판단 기준** — 면접에서 "그건 어떻게 하셨어요?"라고 물으면 답해야 하는 것은 여기에,
> 문서에 적혀 있어야 설득이 되는 것은 공개본에.

---

## NSUS Group — 글로벌 규제 보고 데이터 파이프라인

### 네덜란드 XML 전수 파싱 (1,170만 건)

**실행 환경을 Glue → EMR Serverless로 바꾼 이유**

| | Glue DynamicFrame | EMR Serverless + spark-xml |
|---|---|---|
| `ignoreNamespace` | **미지원** ← 필요했던 것 | 지원 |
| 소형 파일 자동 그룹핑 | **지원** (`groupFiles`) | 없음 |

- Glue XML `format_options` 전체: `rowTag`(필수) · `encoding` · `excludeAttribute` · `treatEmptyValuesAsNulls` · `attributePrefix` · `valueTag` · `ignoreSurroundingSpaces` · `withSchema`
  - `ignoreNamespace`가 없다. 공식 문서는 "behaves **similarly to** databricks/spark-xml"이라고만 표현한다 (동일하지 않음).
- `groupFiles: 'inPartition'` / `groupSize`
  - **DynamicFrame 전용**이며 csv · ion · grokLog · json · xml 에서만 동작 (avro/parquet/orc 미지원)
  - 입력 파일 **50,000개 초과 시 자동 활성화**
  - 일반 Spark DataFrame API에는 없음 → EMR Serverless로 가면 못 씀
- 결론: 소형 파일 최적화를 포기하고 네임스페이스 처리를 택함. 그 대가로 아래 튜닝을 직접 해야 했다.

**driver OOM 해결 — Spark 설정**

```
spark.sql.sources.parallelPartitionDiscovery.threshold = 0
    기본 32. 입력 경로 수가 이 값을 넘으면 파일 리스팅을 executor로 분산한다.
    0으로 두면 항상 분산 → driver 단독 리스팅으로 인한 OOM 해소.

spark.sql.files.maxPartitionBytes = 16MB     (기본 128MB, 소형 파일에 맞춰 축소)
spark.driver.maxResultSize        = 20g
spark.sql.files.openCostInBytes   = 4MB      ※ 노트에 openCostInByte(s 누락)로 적혀 있었음.
                                                오타면 Spark가 조용히 무시한다. 값이 기본값과 같아 실동작 차이는 없음.
```

Glue 노트북 세션 설정 (탐색 단계):

```
%glue_version 4.0
%worker_type G.2X
%number_of_workers 10
--extra-jars s3://.../spark-xml_2.12-0.18.0.jar
```

**XML 네임스페이스 탐색**

- `ignoreNamespace=true`는 **child tag만** 제거하고 **root tag의 네임스페이스는 남긴다.**
  → `rowTag`를 지정하려면 root가 `ns2:Complaint`인지 `ns5:Complaint`인지 먼저 알아야 한다.
- 해결: S3 `GetObject`에 `Range='bytes=0-511'` — 객체 앞 512바이트만 조회
  - XML 선언(`<?xml ...?>`)과 root tag가 문서 맨 앞에 오므로 512바이트면 충분
  - 정규식 `<(ns\d+):` 으로 prefix 추출
  - `ThreadPoolExecutor` 병렬 조회, S3 throttling 방지로 worker 5, botocore `retries={'mode':'adaptive'}`
- 수집한 prefix별로 각각 읽어 `unionByName(allowMissingColumns=True)` 병합

**기각된 가설 — manifest 분할**

S3 list API로 경로만 먼저 뽑아 txt 20개로 나눠 넘기면 driver 리스팅 부담이 사라져 빠를 것이라 가정했으나, 실측 결과 더 느렸다.

| 방식 | HEAD 요청 | 파티션 계획 | 파이프라이닝 | 총 시간 |
|---|---|---|---|---|
| **직접 glob** | 불필요 | 최적화 | O | **35분** |
| Manifest + 콤마 | 50K번 | 순차 | X | 58분+ |
| Manifest + 청크 | 50K×3번 | 순차×3 | X | 더 느림 |

경로를 콤마로 이어 붙여 넘기면 Spark가 파일마다 개별 HEAD 요청을 날리고 파티션 계획도 순차 처리해서, glob으로 넘길 때 타는 병렬 리스팅·최적화 경로를 못 탄다.

**적재 구조**

- Parquet 파티셔닝: `report_type` / `stnd_ymd` 2레벨
- repartition 수는 DataFrame 예상 크기(`_jdf.queryExecution().optimizedPlan().stats().sizeInBytes()`)를 목표 파티션 크기(64MB)로 나눠 동적 계산
- RDB(MySQL) 적재 후 중복 제거 → 인덱스 생성 순서
  - 수동 재전송분이 섞여 있어 record 단위 최신값 판별 필요
  - JSON 파싱 확인: `data->>'$.RecordID'`, `JSON_VALUE(data, '$.RecordID')`
  - JDBC 옵션 `batchsize=10000`, `numPartitions=10`, `isolationLevel=NONE`
  - 실측: 동일 데이터 `numPartitions` 5 → 10 으로 1분 7초 → 32초
- Snowflake 주기 적재

**전체 소요** — 1,170만 건 전수 파싱 약 2시간

---

## 웅진씽크빅 — ETL 파이프라인 최적화

### Spark 자원 산정

executor당 core 수와 메모리를 표준 산정 공식으로 기준값을 잡은 뒤 워크로드에 맞춰 조정.

> 면접 대비 정리 — executor당 core 5개 내외, 노드 자원에서 오버헤드를 뺀 뒤 executor 수로 나눠 메모리 산정, 셔플 파티션은 총 core 수의 2~3배.

### EMR 비용 최적화 실측값

- 동일 스펙 인스턴스 수 조정: 2시간 10분 → 1시간 35분
- Spot 확보량에 따른 구성: 동일 스펙 6시간 → 3시간 50분 / 1.5배 스펙 6시간 → 2시간 20분
- Spot 적용 구간 비용 60% 감소
- On-Demand → Spot + 인스턴스 수 조정으로 특정 워크로드 일 평균 33% 절감
- 전체 월 55% 절감

**1.5배 스펙이 총 비용에서 유리했던 계산**

| 구성 | 수행 시간 | 자원 × 시간 |
|---|---|---|
| 동일 스펙 | 3시간 50분 | 1.0 × 3.83 = 3.83 |
| 1.5배 스펙 | 2시간 20분 | 1.5 × 2.33 = 3.50 |

자원을 1.5배로 올렸는데 총 비용이 약 9% 낮다. 시간이 스펙보다 크게 줄었기 때문.
※ 1.5배 스펙이 정확히 1.5배 단가인지는 미확인 (인스턴스 타입·Spot 비중 변동 여부 확인 필요) — 확인되면 공개본에 넣을 만한 소재.

### EMR 기타

- EMR 5.x → 6.x 버전업으로 Spark 3.x AQE 사용
- Zeppelin → EMR Serverless Notebook 이전
- Graviton(r6g) 도입 검토 — 15% 절감 기대했으나 EMR 기동 오류로 테스트 미완

---

## 비투엔 — SKB 정보계 마이그레이션

### Hive vs Spark 차이로 겪은 것

- **정규식 문법 차이**
  - Hive: `REGEXP_REPLACE(컬럼,'\(\\d+\)\.\(\\d+\)\.\(\\d+\)\[-|.]\(\\d+\)',$1)`
  - PySpark: Python 문법을 따름 → `REGEXP_REPLACE(컬럼,'(\d+)\.(\d+)\.(\d+)-|.',$1)`
  - 이스케이프 규칙이 달라 패턴이 매칭되지 않고 NULL 반환
- **Python 2 → 3 타입 이슈**
  - Oozie는 Python 2로 PySpark job 제출, Airflow에서 Python 3로 제출하니 Long type 에러
  - Python 3에서 `LongType`이 `IntType`으로 통합된 데 따른 것
  - POC 단계라 타입 코드를 전면 수정하지 않고 **런타임 버전을 고정**하여 전환 범위 확대 방지
- **출력 포맷**
  - Hive는 ORC 최적화 (`'ORC.COMPRESS'='SNAPPY'`), Spark는 Parquet 최적화 (`'PARQUET.COMPRESS'='SNAPPY'`)
- **Hive Tez union all** — part별 개수만큼 subdirectory 생성 (쿼리 4개 union all → subdirectory 4개)
- **SparkSQL 제약** — `set hive` 구문 미적용, 세미콜론 단위 파싱이 들여쓰기에 취약해 이후 쿼리 skip

### 기타 설정

- `spark.sql.files.maxPartitionBytes=128m` — file-based source(Parquet/JSON/ORC)에만 적용
- Spark 3.0 Dynamic Allocation: `spark.dynamicAllocation.enabled` / `shuffleTracking.enabled` / `spark.shuffle.service.enabled`
- 자원 배정: 작은 워크로드 3 core + 10GB / 큰 워크로드 5 core + 20GB
- HDFS 블록 128MB (표준) 또는 256MB
- EMR 6.x에서 Hive 워크로드 시 `too many counters` 에러 → EMR 5.33에서 실행하여 회피
- EMR 노드 역할: master(yarn/thrift/hue/oozie) · core(hdfs) · task(job 처리)
  - Spark driver/executor는 core·task 노드에 랜덤 배정. master는 worker로 구성되지 않아 배정 불가
- S3A 사용 — S3N 개선 버전, Amazon 라이브러리 사용, 5GB 이상 파일 지원 및 성능 향상
- 변수 처리: Oozie는 UI 입력, Airflow는 DAG 입력. Hive 변수는 PySpark에서 argparse로 처리
- CDP/CDE 환경 특성: SparkSession 생성 시 `master("yarn")` 제거 (K8s 기반), 잔버그 다수

### 검증하지 못한 것

- MWAA로 EMR job 수행, MSK 스트리밍 처리, Redshift 적재 후 Athena와 쿼리 속도 비교(Redshift 우위) — 검토 수준이라 공개본에서 제외
- CDC 기반 상태성 데이터 변경 처리(dup → 전일자 swap 비교 → DW 적재) — 실제 구현 범위 불명확하여 제외

---

## GS Retail · GS Shop — 비즈니스 메타 표준화

### 중복 진단 룰 구조

업무규칙 37건 (상품 34건 · 고객 3건)

| 레벨 | 판정 축 | 조건식 |
|---|---|---|
| LV1 | 정규화 상품명 | `COUNT(*) OVER(PARTITION BY 상품명) > 1` |
| LV2 | + 판매가 / 협력사 / 소싱구분코드 및 조합 | `EXP_CNT = SUP_CNT` (그룹 **전체**가 동일해야 채택) |
| LV3 | + 담당MD · 상품분류 오매핑 | `EXP_CNT <> MDID_CNT` (그룹 내에서 **갈리면** 오류) |

LV2는 `=`, LV3는 `<>`로 부호가 뒤집힌다. LV2는 "같아야 같은 상품인 게 확실해진다"로 후보를 좁히고, LV3는 "같은 상품인데 담당MD/분류가 다르다"로 오등록을 확정한다.

- 상품명은 `EC_EXPOS_PRD_NM`(EC 노출명) 우선, NULL이면 `PRD_NM`으로 대체 (UNION ALL)
  → 고객이 실제로 보는 이름 기준으로 판정
- 정규화: 괄호·공백·특수문자 제거 후 그룹핑
  → `초코파이 4입` 과 `초코파이(4입)` 을 같은 상품으로 인식
- GSR은 같은 프레임을 `ITEM_NM` + 사업부구분코드(`BD_SP_CD`) + 분류 오매핑으로 이식

### 계열사 간 고객 정합성

- `GSCIADM_TB_CUST_STND`의 CI 회원번호(SERIAL7)를 허브로 GSR(SERIAL1) · GSH(SERIAL9) 연결
- 성별 코드 체계가 달라 정규화 후 비교: GSR `M/F` ↔ GSH `1/2`
- 값 불일치뿐 아니라 **한쪽만 NULL인 편측 결측**을 별도 조건으로 처리 (`NULL <> NULL`은 안 걸리므로)

### 70%의 성격

정규화 상품명 기반 판정이므로 표기 규칙이 크게 다른 상품은 누락되고, 정규화 후 이름이 같아도 실제로는 다른 상품인 경우가 있다. **추정치이며 확정 수치가 아니다.**

---

## Airbyte 전환 — cursor 증분 적재

### `>` 대신 `>=` 를 쓴 이유

날짜 컬럼의 소수점 정밀도가 낮아 동일 시각 값을 가진 row가 다수 존재할 때:

| 조건 | 다음 배치 | 결과 |
|---|---|---|
| `> T` (초과) | `T`보다 **큰** 값부터 | 같은 시각의 남은 row는 **영영 안 읽힘 → 누락** |
| `>= T` (이상) | `T`부터 다시 | 전부 읽음 → 중복 발생 → **dedup이 제거** |

중복은 dedup으로 지울 수 있지만 누락은 나중에 알아차려도 복구가 안 된다. 비대칭이라 `>=`가 유일한 선택.

### `>=` 로도 못 막는 것 (면접 대비)

**늦게 커밋되는 트랜잭션.** 배치보다 먼저 시작됐지만 나중에 커밋되는 row는 timestamp가 기준점보다 과거인데 배치 시점에 안 보인다.

```
10:00  트랜잭션 시작 (created_at = 10:00)
10:05  배치 실행 → 기준점 10:05 로 갱신     ← 이 row는 아직 안 보임
10:07  트랜잭션 커밋                          ← 이제 보이지만 10:00 < 10:05
11:05  다음 배치: >= 10:05 → 못 읽음
```

timestamp 기반 증분 추출의 구조적 한계. lookback window(기준점에서 N분 물러나 다시 읽기)를 두거나 CDC/CT로 가야 한다.
→ 실제로 lookback을 뒀는지 미확인. 안 뒀다면 한계로 적을 소재.

---

## 확인·보완이 필요한 항목

- [ ] Airbyte cursor — lookback window 적용 여부
- [ ] 웅진 1.5배 스펙의 실제 단가 배수 (정확히 1.5배인지)
- [ ] DataHub — 확정 정의 비율, 카탈로그 사용자 수
- [ ] ETL 최적화 — 대상 ETL 작업 수, DAG 수
- [ ] Airflow K8s — 디스크 포화 발생 빈도 before/after, DAG 배포 소요 시간 before/after
- [ ] 규제 대응 — inquiry 대응 소요 시간 before/after, 정정 처리 건수
- [ ] AICC — VOC 대응 건수 before/after
- [ ] 비투엔 2021.09 ~ 2022.09 (약 1년) 공백 프로젝트 — 면접 답변 준비
- [ ] 웅진 2023.05 ~ 2023.11 (7개월), 2024.10 ~ 2024.12 (3개월) — 면접 답변 준비
- [ ] 2025.01 ~ 2025.08 휴직 8개월 — 면접 답변 준비
