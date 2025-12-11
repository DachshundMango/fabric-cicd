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


tables = ["energy_finance_analysis", "energy_demand", "yfinance"] 

for table in tables:

    table_name = str("lh_australia_energy." + table)

    df = spark.table(table_name)

    save_path = f"Files/download_temp/{table}"

    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(save_path)

    print(f"saved {table} successfully. path: {save_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Hello! Fabric and Azure!")
print("GitHub test!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
