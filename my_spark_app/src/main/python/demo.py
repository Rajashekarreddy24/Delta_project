from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import split, monotonically_increasing_id
from pyspark.sql.functions import col


builder = SparkSession.builder.appName('Test')\
    .config('spark.driver.bindAdress', 'http://10.0.2.15:4041')\
    .config('spark.ui.port', '4041')\
    .config('spark.driver.extraClasspath', '/home/hadoop/jdbc-drivers/postgresql-42.6.0.jar')
   
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()

db_url = f"jdbc:postgresql://e-commerce.cj3oddyv0bsk.us-west-1.rds.amazonaws.com:5432/night_audits"
df = spark.read \
 .format("jdbc") \
 .option("url", db_url) \
 .option("dbtable", "ar_aging_detail_report") \
 .option("user", "postgres") \
 .option("password", "Welcome!234") \
 .option("driver", "org.postgresql.Driver") \
 .load()

df.show()




