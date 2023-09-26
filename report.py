from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import split, monotonically_increasing_id



builder = SparkSession.builder.appName('Test')\
    .config('spark.driver.bindAdress', 'http://10.0.2.15:4040')\
    .config('spark.ui.port', '4040')\
   
    
spark = configure_spark_with_delta_pip(builder).getOrCreate()



path = r'/home/hadoop/Desktop/Delta_project/AR-Aging-Detail-Report.csv'
target_path = r'home/hadoop/Desktop/test'

print('spark session started...............')


data = spark.read.options( header = True, delimiter=' ').option('inferSchema', 'true').csv(path)
data.show()
null_dropped = data.na.drop()
d1 = null_dropped.withColumn('index', monotonically_increasing_id())
d1.write.format('csv').mode('overwrite').save(target_path)
d1.show()
# row_to_drop = [0,1,2]
# row_drop = d1.filter(d1.index.isin(row_to_drop))
# row_drop.write.format('csv').mode('overwrite').save(target_path)
# row_drop.show()



# row_drop.write.format('csv').mode('overwrite').save(target_path)
# row_drop.show()



# null_dropped.createTempView('test')
# df = spark.sql("select * from test ")
# print(df)
# df.show()

# null_dropped.drop()

# split_col = split(null_dropped['values'], ' ')

# df = null_dropped.withColumn('col1', split_col.getItem[0])



# dat = spark.range(0,5)
# dat.write.format('delta').mode('overwrite').save(target_path)
# delta_table = spark.read.format('delta').load(target_path)


