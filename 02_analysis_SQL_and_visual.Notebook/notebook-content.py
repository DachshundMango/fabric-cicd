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

df = spark.table("lh_australia_energy.energy_finance_analysis")

df.createOrReplaceTempView("energy")

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query_hourly = """
    SELECT
        HOUR(SETTLEMENTDATE) as Hour,
        ROUND(AVG(TOTALDEMAND), 0) as Avg_Demand,
        ROUND(AVG(RRP), 2) as Avg_Price,
        ROUND(AVG(AUDUSD_X), 4) as Avg_FX
    FROM energy
    GROUP BY HOUR(SETTLEMENTDATE)
    ORDER BY Avg_Demand
"""

df_hourly = spark.sql(query_hourly)
display(df_hourly)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query_monthly = """
    SELECT
        MONTH(SETTLEMENTDATE) as Month,
        HOUR(SETTLEMENTDATE) as Hour,
        ROUND(AVG(TOTALDEMAND), 0) as Avg_Demand,
        ROUND(AVG(RRP), 2) as Avg_Price,
        ROUND(AVG(AUDUSD_X), 4) as Avg_FX
    FROM energy
    WHERE MONTH(SETTLEMENTDATE) IN (1, 11)
    GROUP BY MONTH(SETTLEMENTDATE), HOUR(SETTLEMENTDATE)
    ORDER BY Avg_Price
"""

df_monthly = spark.sql(query_monthly)
display(df_monthly)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query_negative = """
    SELECT
        HOUR(SETTLEMENTDATE) as Hour,
        COUNT(*) as Total_Count,
        SUM(CASE WHEN RRP < 0 THEN 1 ELSE 0 END) as Negative_Count
    FROM energy
    GROUP BY HOUR(SETTLEMENTDATE)
    ORDER BY Hour
"""

df_negative = spark.sql(query_negative)
display(df_negative)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

query_fx = """
    SELECT

        CASE
            WHEN AUDUSD_X >= 0.64 THEN 'High FX'
            ELSE 'Low FX'
        END as FX_Group,

        ROUND(AVG(RRP), 2) as Avg_Price,
        ROUND(AVG(TOTALDEMAND), 0) as Avg_Demand
    
    FROM energy
    GROUP BY 
        CASE
            WHEN AUDUSD_X >= 0.64 THEN 'High FX'
            ELSE 'Low FX'
        END 
    ORDER BY FX_Group
"""

df_fx = spark.sql(query_fx)
display(df_fx)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.table("lh_australia_energy.energy_finance_analysis")
df_report = df.withColumn("Hour", F.hour("SETTLEMENTDATE")).withColumn("FX_Group", F.when(F.col("AUDUSD_X") >= 0.64, "High FX").otherwise("Low FX"))

df_report.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable("lh_australia_energy.energy_finance_analysis")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
