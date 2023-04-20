# BigData_TGS5

## Analitik dengan DataFrames

cd /home/cloudera/spark-2.0.0-bin-hadoop2.7/sbin
sudo ./start-all.sh

Kode 1
mylist = [(50, "DataFrame"),(60, "pandas")]
myschema = ['col1', 'col2']
Metode 1: Membuat DataFrame dengan objek list, schema dan default data types
Kode 2
df1 = spark.createDataFrame(mylist, myschema)
Metode 2: Membuat DataFrame dengan parallelizing list dan konversi RDD ke DataFrame
Kode 3
df2 = sc.parallelize(mylist).toDF(myschema)
Method 3: Read data from a file, Infer schema and convert to DataFrame
Copy file people.txt yang terletak di folder examples/resources ke hdfs. Lakukan perintah berikut di tab terminal lain dengan perintah hadoop seperti berikut.

Kode 4
hadoop fs -put /examples/resources/people.txt people.txt 
Kemudian kembali ke tab terminal pyspark shell, jalankan kode python berikut.

Kode 5
from pyspark.sql import SQLContext, Row
peopleRDD = sc.textFile("people.txt")
people_sp = peopleRDD.map(lambda l: l.split(","))
people = people_sp.map(lambda p: Row(name=p[0], age=int(p[1])))
df_people = spark.createDataFrame(people)
df_people.createOrReplaceTempView("people")
spark.sql("SHOW TABLES").show()
spark.sql("SELECT name,age FROM people where age > 19").show() 
Metode 4: Membaca data dari file, lalu assign schema secara programmatically
Kode 6
from pyspark.sql import SQLContext, Row
peopleRDD = sc.textFile("people.txt")
people_sp = peopleRDD.map(lambda l: l.split(","))
people = people_sp.map(lambda p: Row(name=p[0], age=int(p[1])))
df_people = people_sp.map(lambda p: (p[0], p[1].strip()))
schemaStr = "name age"
fields = [StructField(field_name, StringType(), True) \
for field_name in schemaStr.split()]
schema = StructType(fields)
df_people = spark.createDataFrame(people,schema)
df_people.show()
df_people.createOrReplaceTempView("people")
spark.sql("select * from people").show() 
