from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import split, monotonically_increasing_id
from pyspark.sql.functions import col


builder = SparkSession.builder.appName('Test')\
    .config('spark.driver.bindAdress', 'http://10.0.2.15:4041')\
    .config('spark.ui.port', '4041')\
   
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()



path = r'/home/hadoop/Desktop/Delta_project/AR-Aging-Detail-Report.csv'
target_path = r'home/hadoop/Desktop/test'
de_path = r'home/hadoop/Desktop'

print('spark session started...............')


data = spark.read.options( header = True, delimiter=' ').option('inferSchema', 'true').csv(path)
data1 = data.na.drop()
data1.write.format('csv').mode('overwrite').save(target_path)
data1.show()
col_split = data1.withColumn('Invoice number', split(data['A/R Aging Detail Report'],' ').getItem(0))\
    .withColumn('Invoice date', split(data['A/R Aging Detail Report']," ").getItem(1))\
    .withColumn('Current', split(data['A/R Aging Detail Report'],' ').getItem(2))\
    .withColumn('30 days', split(data['A/R Aging Detail Report'],' ').getItem(3))\
    .withColumn('60 days', split(data['A/R Aging Detail Report'],' ').getItem(4))\
    .withColumn('90 days', split(data['A/R Aging Detail Report'],' ').getItem(5))\
    .withColumn('120 days',split(data['A/R Aging Detail Report'],' ').getItem(6))\
    .withColumn('Credits', split(data['A/R Aging Detail Report'],' ').getItem(7))\
    .withColumn('Balance', split(data['A/R Aging Detail Report'],' ').getItem(8))\
    # .drop('A/R Aging Detail Report')
    

d1 = col_split.withColumn('index', monotonically_increasing_id())


row_drop = d1.filter(d1.index != '0').filter(d1.index != '1').filter(d1.index != '2')\
    .filter(d1.index != '24').filter(d1.index != '25').filter(d1.index != '26').filter(d1.index != '29').filter(d1.index != '30').filter(d1.index != '31')\
        .filter(d1.index != '32').filter(d1.index != '33').filter(d1.index != '34').filter(d1.index != '35').filter(d1.index != '62').filter(d1.index != '63').filter(d1.index != '64').filter(d1.index != '65')\
            .filter(d1.index != '66').filter(d1.index != '67').filter(d1.index != '68').filter(d1.index != '69').filter(d1.index != '72').filter(d1.index != '73').filter(d1.index != '76').filter(d1.index != '77').filter(d1.index != '78')

row_drop.write.format('csv').mode('overwrite').save(de_path)
row_drop.show()
d2 = spark.read.options(header = False).csv(de_path)


def drop_empty_columns(d2):
    non_empty_columns = [col for col_name in d2.columns if d2.select(col_name).dropna().count() > 0]
    return d2.select(*non_empty_columns)

df_filtered = drop_empty_columns(d2)


# table_name = 'ar_aging_report'
# db_url = "jdbc:postgresql://your_postgresql_host:5432/your_database_name"
# db_properties = {
#     "user": "your_username",
#     "password": "your_password",
#     "driver": "org.postgresql.Driver"
# }

# table_name = "your_table_name"  
# d2.write.jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)