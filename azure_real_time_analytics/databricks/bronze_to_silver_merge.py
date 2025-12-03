# Databricks PySpark CDC MERGE (bronze -> silver)
from pyspark.sql.functions import col, current_timestamp

bronze = spark.read.format("delta").load("/mnt/bronze/claims")
silver_path = "/mnt/silver/claims"

# Upsert logic (assumes _change_type in bronze)
bronze.createOrReplaceTempView("bronze")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS claims_silver
USING DELTA
LOCATION '{silver_path}'
AS SELECT * FROM bronze WHERE 1=0
""")

spark.sql("""
MERGE INTO claims_silver AS tgt
USING bronze AS src
ON tgt.claim_id = src.claim_id
WHEN MATCHED AND src._change_type = 'update' THEN UPDATE SET *
WHEN MATCHED AND src._change_type = 'delete' THEN DELETE
WHEN NOT MATCHED AND src._change_type IN ('insert','update') THEN INSERT *
""")
