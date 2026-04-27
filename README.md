from pyspark.sql.functions import *
from pyspark.sql import *

spark = SparkSession.builder.appName("Python").master("local[2]").getOrCreate()

data=r"C:\bigdata\drivers\world_bank.json"
data1=r"C:\bigdata\drivers\jsonfiles\simple.json"
df = spark.read.format("json").option("mode","DROPMALFORMED").option("multiline","true").load(data1)
df.printSchema()

data2=r"C:\bigdata\drivers\jsonfiles\jsonarray.json"
df2 = spark.read.format("json").option("multiline","true").load(data2)
df2.printSchema()
df2=df2.withColumn("supportedLanguages",explode("supportedLanguages"))
df2.show()

data3=r"C:\bigdata\drivers\jsonfiles\structjson.json"
df3 = spark.read.format("json").option("multiline","true").load(data3)
df3=df3.withColumn("products",explode("products"))
df3.printSchema()
df_flat = (
    df3.withColumn("id", col("products.id"))
       .withColumn("name", col("products.name"))
       .withColumn("price", col("products.price"))
       .withColumn("inStock", col("products.inStock"))
       .drop("products")
)
df_flat.show()
##df.withColumn("majorsector_percent",explode("majorsector_percent"))
##df.withColumn("mjsector_namecode",explode("mjsector_namecode"))
