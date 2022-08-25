# Ways of creating RDDs
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext

# 1) Using Parallelize method
# data=[(1,"Aditya",70000,"Hyderabad"),(2,"Neha",80000,"Pune"),(3,"Jaspreet",75000,"Chandigarh"),(4,"Manasi",80000,"Bangalore")]
# srdd=sc.parallelize(data)
# res=srdd.collect()
# #print(res)
# for i in res:
#     print(i)
#
# 2). Using textFile() method
data="D:\\DataSets\\asl.csv"
aslrdd=sc.textFile(data)
skip=aslrdd.first()
# res = aslrdd.filter(lambda x:x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],x[1],x[2])).filter(lambda x: "blr" in x)
# res = aslrdd.filter(lambda x: x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[1]),x[2])).filter(lambda x: x[1]==29 or x[1]==99)
res = aslrdd.filter(lambda x: x!=skip).map(lambda x:x.split(",")).map(lambda x:(x[0],int(x[1]),x[2])).filter(lambda x: x[1] in (99,29))
for i in res.collect():
    print(i)


