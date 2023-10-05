from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import split, monotonically_increasing_id
from pyspark.sql.functions import col, lit, when, row_number
from pyspark.sql import DataFrameReader
import re
from pyspark.sql.types import * 
from pyspark.sql.window import Window

builder = SparkSession.builder.appName('Test')\
    .config('spark.driver.bindAdress', 'http://10.0.2.15:4041')\
    .config('spark.ui.port', '4041')\
    .config('spark.driver.extraClasspath', '/home/hadoop/postgresql-42.6.0.jar')
 
   
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()



path = r'/home/hadoop/Desktop/Delta_project/AR-Aging-Detail-Report.csv'
target_path = r'home/hadoop/Desktop/test'
de_path = r'home/hadoop/Desktop'

print('spark session started...............')


data = spark.read.options( header = True, delimiter=' ').option('inferSchema', 'true').csv(path)
data1 = data.na.drop()
data1.write.format('csv').mode('overwrite').save(target_path)
# data1.show()

# window_spec = Window.orderBy('invoice number')
# row = data1.withColumn("row_number", row_number().over(window_spec))
# condition1 = (data1["row_number"]<=10)
# condition2 = (data1['row_number']>10)
# condition3= (data1['row_number']>=25)


split_pattern = r'\s{1,50}'
col_split = data1.withColumn('Invoice number', split(data['A/R Aging Detail Report'],split_pattern).getItem(0).cast("int"))\
    .withColumn('Invoice date', split(data['A/R Aging Detail Report'],split_pattern).getItem(1))\
    .withColumn('Current', split(data['A/R Aging Detail Report'],split_pattern).getItem(2).cast('int'))\
    .withColumn('30 days', split(data['A/R Aging Detail Report'],split_pattern).getItem(3).cast('int'))\
    .withColumn('60 days', split(data['A/R Aging Detail Report'],split_pattern).getItem(4).cast('int'))\
    .withColumn('90 days', split(data['A/R Aging Detail Report'],split_pattern).getItem(5).cast('int'))\
    .withColumn('120 days',split(data['A/R Aging Detail Report'],split_pattern).getItem(6).cast('int'))\
    .withColumn('Credits', split(data['A/R Aging Detail Report'],split_pattern).getItem(7).cast('int'))\
    .withColumn('Balance', split(data['A/R Aging Detail Report'],split_pattern).getItem(8).cast('int'))\
    .withColumn('Account Number',lit('651045'))\
    .drop('A/R Aging Detail Report').show()
    
# print('Printing seconf Column')
# print("-----------")
# second_col.select('*').collect()


# d1 = col_split.withColumn('index', monotonically_increasing_id())
# # d1.write.format('csv').mode('overwrite').save(de_path)
# # d1.show()


# row_drop = d1.filter(d1.index != '0').filter(d1.index != '1').filter(d1.index != '2')\
#     .filter(d1.index != '24').filter(d1.index != '25').filter(d1.index != '26').filter(d1.index != '29').filter(d1.index != '30').filter(d1.index != '31')\
#         .filter(d1.index != '32').filter(d1.index != '33').filter(d1.index != '34').filter(d1.index != '35').filter(d1.index != '64').filter(d1.index != '65')\
#             .filter(d1.index != '66').filter(d1.index != '67').filter(d1.index != '68').filter(d1.index != '69').filter(d1.index != '76').filter(d1.index != '77').filter(d1.index != '78')


# row_drop.write.format('csv').mode('overwrite').save(de_path)

# row_drop.show()

# df = row_drop.drop('index')

# table_name = 'ar_aging_detail_report'
# db_url = "jdbc:postgresql://e-commerce.cj3oddyv0bsk.us-west-1.rds.amazonaws.com:5432/night_audits"
# db_properties = {
#     "user": "postgres",
#     "password": "Welcome!234",
#     "driver": "org.postgresql.Driver"
# }
# df.write.jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)
# print('file uploaded into Db')

# spark.stop()

