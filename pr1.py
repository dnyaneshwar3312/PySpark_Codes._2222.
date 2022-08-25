from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext
#data=[12,45,16,18,19,23]
#drdd=spark.sparkContext.parallelize(data)
data="D:\\DataSets\\drivers\\asl.csv"
aslrdd=sc.textFile(data)
skipp=aslrdd.first()
res=aslrdd.filter(lambda x: x!=skipp).map(lambda x:x.split(",")).filter(lambda x: int(x[1])>30)

for i in res.collect():
    print(i)
