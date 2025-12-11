# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c3f4be21-988d-42a0-9b70-8816a5c9aa09",
# META       "default_lakehouse_name": "lh_australia_energy",
# META       "default_lakehouse_workspace_id": "d6dd24d2-7c72-461c-a092-ef167ecc8818",
# META       "known_lakehouses": [
# META         {
# META           "id": "c3f4be21-988d-42a0-9b70-8816a5c9aa09"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

# 1. Read tables
df_energy = spark.table('lh_australia_energy.energy_demand')
df_finance = spark.table('lh_australia_energy.yfinance')

# 2. Date column transition + ordering
df_energy_processed = df_energy.withColumn("SETTLEMENTDATE", F.to_timestamp('SETTLEMENTDATE', "yyyy/MM/dd HH:mm:ss"))
df_energy_final = df_energy_processed.withColumn("JoinDate", F.to_date('SETTLEMENTDATE', "yyyy/MM/dd HH:mm:ss")).orderBy('SETTLEMENTDATE')

# 3. Left join
df_merged = df_energy_final.join(df_finance, on=df_energy_final.JoinDate==df_finance.Date, how='left').orderBy('SETTLEMENTDATE')

# 4. Result
display(df_merged.limit(10))

# 5. Save it to Delta table
df_merged.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("lh_australia_energy.energy_finance_analysis")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### **1. 테이블 읽기 (Reading Tables)**
# 
# ```python
# df = spark.table('database_name.table_name')
# ```
# - **설명**: Delta Lake나 Hive 메타스토어에 저장된 테이블을 DataFrame으로 읽어옴
# - **예제**: `spark.table('lh_australia_energy.energy_demand')`
# 
# ---
# 
# ### **2. 컬럼 추가/변환 (Column Operations)**
# 
# #### **2.1 `withColumn()`**
# ```python
# df.withColumn("new_column_name", expression)
# ```
# - **설명**: 새 컬럼을 추가하거나 기존 컬럼을 변환
# - **특징**: 원본 DataFrame은 변경하지 않고 새로운 DataFrame 반환 (immutable)
# 
# **예제:**
# ```python
# # 날짜 형식 변환
# df.withColumn("JoinDate", F.to_date('SETTLEMENTDATE', "yyyy/MM/dd HH:mm:ss"))
# 
# # 새 컬럼 생성
# df.withColumn("Year", F.year("SETTLEMENTDATE"))
# df.withColumn("Total", F.col("Price") * F.col("Quantity"))
# ```
# 
# ---
# 
# ### **3. 날짜 함수 (Date Functions)**
# 
# #### **3.1 `F.to_date()`**
# ```python
# F.to_date(column, format)
# ```
# - **설명**: 문자열을 날짜(Date) 타입으로 변환
# - **파라미터**:
#   - `column`: 변환할 컬럼명
#   - `format`: 원본 데이터의 날짜 형식
# 
# **포맷 패턴:**
# - `yyyy`: 년도 (4자리)
# - `MM`: 월 (01-12)
# - `dd`: 일 (01-31)
# - `HH`: 시간 (00-23)
# - `mm`: 분 (00-59)
# - `ss`: 초 (00-59)
# 
# **예제:**
# ```python
# # "2024/12/09 14:30:00" → Date
# F.to_date('SETTLEMENTDATE', "yyyy/MM/dd HH:mm:ss")
# 
# # "2024-12-09" → Date
# F.to_date('date_string', "yyyy-MM-dd")
# ```
# 
# ---
# 
# ### **4. 정렬 (Sorting)**
# 
# #### **4.1 `orderBy()` / `sort()`**
# ```python
# df.orderBy('column_name')  # 오름차순
# df.orderBy(F.col('column_name').desc())  # 내림차순
# ```
# - **설명**: DataFrame을 특정 컬럼 기준으로 정렬
# - **특징**: `orderBy()`와 `sort()`는 동일한 기능
# 
# **예제:**
# ```python
# # 단일 컬럼 정렬
# df.orderBy('SETTLEMENTDATE')
# 
# # 여러 컬럼 정렬
# df.orderBy('Year', 'Month')
# 
# # 내림차순
# df.orderBy(F.col('Price').desc())
# ```
# 
# ---
# 
# ### **5. 조인 (Joins)**
# 
# #### **5.1 `join()`**
# ```python
# df1.join(df2, join_condition, how='join_type')
# ```
# 
# **파라미터:**
# - `df2`: 조인할 DataFrame
# - `join_condition`: 조인 조건
# - `how`: 조인 타입
# 
# **조인 타입:**
# - `'inner'`: 양쪽 모두 매칭되는 행만 (기본값)
# - `'left'` / `'left_outer'`: 왼쪽 DataFrame의 모든 행 유지
# - `'right'` / `'right_outer'`: 오른쪽 DataFrame의 모든 행 유지
# - `'outer'` / `'full'` / `'full_outer'`: 양쪽 모든 행 유지
# 
# **예제:**
# ```python
# # Left Join
# df_merged = df1.join(df2, 
#                      df1.JoinDate == df2.Date, 
#                      how='left')
# 
# # Inner Join (컬럼명이 같을 때)
# df_merged = df1.join(df2, on='common_column', how='inner')
# 
# # 여러 조건
# df_merged = df1.join(df2, 
#                      (df1.Date == df2.Date) & (df1.Region == df2.Region),
#                      'inner')
# ```
# 
# ---
# 
# ### **6. 결과 확인 (Display/Preview)**
# 
# #### **6.1 `limit()`**
# ```python
# df.limit(n)
# ```
# - **설명**: 상위 n개 행만 반환
# - **예제**: `df.limit(10)`
# 
# #### **6.2 `display()` (Databricks/Fabric 전용)**
# ```python
# display(df)
# ```
# - **설명**: 노트북에서 DataFrame을 테이블 형태로 시각화
# - **대안** (일반 PySpark): `df.show()`
# 
# **예제:**
# ```python
# display(df.limit(10))  # 상위 10개 행 표시
# df.show(5)  # 콘솔에 5개 행 출력
# ```
# 
# ---
# 
# ### **7. 데이터 저장 (Write Operations)**
# 
# #### **7.1 `write.format().mode().saveAsTable()`**
# ```python
# df.write.format("format_type").mode("write_mode").saveAsTable("table_name")
# ```
# 
# **파라미터:**
# - `format()`: 저장 형식
#   - `"delta"`: Delta Lake (권장)
#   - `"parquet"`: Parquet
#   - `"csv"`: CSV
#   
# - `mode()`: 쓰기 모드
#   - `"overwrite"`: 기존 테이블 덮어쓰기
#   - `"append"`: 기존 테이블에 추가
#   - `"error"` / `"errorifexists"`: 테이블 존재시 에러 (기본값)
#   - `"ignore"`: 테이블 존재시 아무것도 안 함
# 
# **예제:**
# ```python
# # Delta 테이블로 저장 (덮어쓰기)
# df.write.format("delta").mode("overwrite").saveAsTable("database.table_name")
# 
# # Parquet 파일로 저장
# df.write.format("parquet").mode("append").save("/path/to/file.parquet")
# 
# # CSV 파일로 저장
# df.write.format("csv").option("header", True).mode("overwrite").save("/path/to/file.csv")
# ```
# 
# ---
# 
# ### **8. 추가 유용한 메서드들**
# 
# #### **8.1 데이터 확인**
# ```python
# df.count()              # 행 개수
# df.columns              # 컬럼명 리스트
# df.printSchema()        # 스키마 출력
# df.describe().show()    # 기술통계량
# ```
# 
# #### **8.2 필터링**
# ```python
# df.filter(F.col("Price") > 100)
# df.where("Region = 'NSW'")
# ```
# 
# #### **8.3 컬럼 선택**
# ```python
# df.select("col1", "col2")
# df.drop("unnecessary_column")
# ```

