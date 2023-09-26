from pyspark.sql.session import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from delta import *
import shutil
from delta.tables import DeltaTable


path = r'/home/hadoop/Desktop/Delta_project/AR Aging Detail Report.csv'
Table_path = r"/home/hadoop/Desktop/test"


builder = SparkSession.builder.master("local[*]").appName(name='Delta_app')\
    .config('spark.driver.bindAdress', 'http://10.0.2.15:4041')\
    .config('spark.ui.port', '4041')\
    .config('spark.driver.port','33563')
spark = configure_spark_with_delta_pip(builder).getOrCreate()

print('Spark is runing ..............')


raw_data = spark.read.options(delimiter=' ', header = True).csv(path)



drop_column = raw_data.drop('Detail').write.mode('overwrite').csv(Table_path)

final_data = spark.read.format('csv').load(Table_path)

delta_table = final_data.write.format('delta').mode('overwrite').save(Table_path)


df = spark.read.format("delta").load(Table_path)

df.show()


# data = DeltaTable.convertToDelta(spark, f"parquet.{Table_path}")
