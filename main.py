from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import split, monotonically_increasing_id



builder = SparkSession.builder.appName('Test')\
    .config('spark.driver.bindAdress', 'http://10.0.2.15:4040')\
    .config('spark.ui.port', '4040')\
   
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()



path = r'/home/hadoop/Desktop/Delta_project/AR-Aging-Detail-Report.csv'
target_path = r'home/hadoop/Desktop/test'
de_path = r'home/hadoop/Desktop'

print('spark session started...............')


data = spark.read.options(delimiter= ' ',header = True).csv(path)

data_index = data.withColumn('index', monotonically_increasing_id())
# data_index.write.format('csv').mode('overwrite').save(target_path)
# data_index.show()

row_drop = data_index.filter(data_index.index != '0').filter(data_index.index != '1').filter(data_index.index != '2')\
    .filter(data_index.index != '3').filter(data_index.index != '4').filter(data_index.index != '26').filter(data_index.index != '27').filter(data_index.index != '28').filter(data_index.index != '29')\
        .filter(data_index.index != '32').filter(data_index.index != '33').filter(data_index.index != '34').filter(data_index.index != '35').filter(data_index.index != '36').filter(data_index.index != '37')\
            .filter(data_index.index != '38').filter(data_index.index != '39').filter(data_index.index != '40').filter(data_index.index != '41').filter(data_index.index != '42').filter(data_index.index != '71')\
                .filter(data_index.index != '71').filter(data_index.index != '72').filter(data_index.index != '73').filter(data_index.index != '74').filter(data_index.index != '75').filter(data_index.index != '76')\
                    .filter(data_index.index != '77').filter(data_index.index != '78').filter(data_index.index != '79').filter(data_index.index != '86').filter(data_index.index != '91')
nulls = row_drop.na.drop()
row_drop.write.format('csv').mode('overwrite').save(target_path)

d1 = spark.read.format('csv').load(target_path)
d1.na.drop().show()

col_split = d1.withColumn('Invoice number', split(data['A/R Aging Detail Report'],' ').getItem(0))\
    .withColumn('Invoice date', split(data['A/R Aging Detail Report']," ").getItem(1))\
    .withColumn('Current', split(data['A/R Aging Detail Report'],' ').getItem(2))\
    .withColumn('30 days', split(data['A/R Aging Detail Report'],' ').getItem(3))\
    .withColumn('60 days', split(data['A/R Aging Detail Report'],' ').getItem(4))\
    .withColumn('90 days', split(data['A/R Aging Detail Report'],' ').getItem(5))\
    .withColumn('120 days',split(data['A/R Aging Detail Report'],' ').getItem(6))\
    .withColumn('Credits', split(data['A/R Aging Detail Report'],' ').getItem(7))\
    .withColumn('Balance', split(data['A/R Aging Detail Report'],' ').getItem(8)).show()
#     # .drop('A/R Aging Detail Report')
    


# # data.show()
# # null_dropped = col_split.na.drop()
# data_index = col_split.withColumn('index', monotonically_increasing_id())
# # data_index.write.format('csv').mode('overwrite').save(target_path)
# # data_index.show()
# # data_index.write.format('csv').mode('overwrite').save(target_path)

# row_drop = data_index.filter(data_index.index != '0').filter(data_index.index != '1').filter(data_index.index != '2')\
#     .filter(data_index.index != '24').filter(data_index.index != '25').filter(data_index.index != '26').filter(data_index.index != '29').filter(data_index.index != '30').filter(data_index.index != '31')\
#         .filter(data_index.index != '32').filter(data_index.index != '33').filter(data_index.index != '62').filter(data_index.index != '63').filter(data_index.index != '64').filter(data_index.index != '65')\
#             .filter(data_index.index != '72').filter(data_index.index != '73')

# row_drop.write.format('csv').mode('overwrite').save(de_path)
# row_drop.show()
# d2 = spark.read.csv(de_path).show()
# print('Updated one.')


# db_url = "jdbc:postgresql://your_postgresql_host:5432/your_database_name"
# db_properties = {
#     "user": "your_username",
#     "password": "your_password",
#     "driver": "org.postgresql.Driver"
# }

# table_name = "your_table_name"  # The name of the table in your PostgreSQL database
# df.write.jdbc(url=db_url, table=table_name, mode="overwrite", properties=db_properties)

