from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import split, monotonically_increasing_id
from pyspark.sql.functions import col,lit


builder = SparkSession.builder.master("local[*]").appName('Test')\
    .config('spark.driver.bindAdress', 'http://10.0.2.15:4041')\
    .config('spark.ui.port', '4041')\
    .config('spark.driver.extraClasspath', '/home/hadoop/postgresql-42.6.0.jar')    
spark = configure_spark_with_delta_pip(builder).getOrCreate()

print('spark is running ..........................')

path = r'/home/hadoop/Desktop/Delta_project/updated.csv'

df = spark.read.options(header = True).csv(path)
df.show()

db_url = f"jdbc:postgresql://e-commerce.cj3oddyv0bsk.us-west-1.rds.amazonaws.com:5432/night_audits"
# # df = spark.read \
# #  .format("jdbc") \
# #  .option("url", db_url) \
# #  .option("dbtable", "ar_aging_detail_report") \
# #  .option("user", "postgres") \
# #  .option("password", "Welcome!234") \
# #  .option("driver", "org.postgresql.Driver") \
# #  .load()
 
# # df.show()

db_properties =  {
    'user':"postgres", 
    'password':'Welcome!234',
    'driver': 'org.postgresql.Driver' 
}

df_col=df.withColumn('Account Number',lit(1234))
df_col = df.drop('balance_amount_of_ptd')
df_col.write.jdbc(url= db_url,table= 'ar_aging_detail_report',mode= 'overwrite', properties= db_properties)
df_col.show()
print('demo file is printing')
spark.stop()

