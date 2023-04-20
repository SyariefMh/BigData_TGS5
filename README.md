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

## Membuat DataFrame dari Database Eksternal
df1 = spark.read.format('jdbc').options(url='jdbc:mysql://ebt-polinema.id:3306/polinema_pln?user=ebt&password=EBT@2022@pltb', dbtable='t_wind_turbine').load()
df1.show()
Metode 2
Kode 8
df2 = spark.read.format('jdbc').options(url='jdbc:mysql://ebt-polinema.id:3306/polinema_pln', dbtable='t_wind_turbine', user='ebt', password='EBT@2022@pltb').load()
df2.show()

## Mengonversi DataFrames ke RDDs
# Create DataFrame
mylist = [(1, "Nama-NIM"),(3, "Big Data 2023")]
myschema = ['col1', 'col2']
df = spark.createDataFrame(mylist, myschema)

#Convert DF to RDD
df.rdd.collect()

df2rdd = df.rdd
df2rdd.take(2)

## Membuat Datasets
case class Dept(dept_id: Int, dept_name: String)

val deptRDD = sc.makeRDD(Seq(Dept(1,"Sales"),Dept(2,"HR")))

val deptDS = spark.createDataset(deptRDD)

val deptDF = spark.createDataFrame(deptRDD)
Perhatian: Ketika Anda melakukan konversi dari Dataset ke RDD, maka akan diperoleh data berupa RDD[Dept]. Tapi, ketika konversi dari DataFrame ke RDD, makan akan diperoleh RDD[Row].

Kode 11
deptDS.rdd

//res12: org.apache.spark.rdd.RDD[Dept] = MapPartitionsRDD[5] at rdd at 
//<console>:31

deptDF.rdd

//res13: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = //MapPartitionsRDD[8] at rdd at <console>:31

deptDS.filter(x => x.dept_location > 1).show()
  
## Mengonversi DataFrame ke Datasets dan sebaliknya
val newDeptDS = deptDF.as[Dept]

newDeptDS.show()

newDeptDS.first()

// mengonversi ke DataFrame kembali
newDeptDS.toDF.first()
  
## Mengakses Metadata menggunakan Catalog
spark.catalog.listDatabases().select("name").show()

spark.catalog.listTables.show()

spark.catalog.isCached("sample_07")

spark.catalog.listFunctions().show()
  
## Bekerja dengan berkas teks
df_txt = spark.read.text("people.txt")
df_txt.show()
df_txt
  
## Bekerja dengan JSON
df_json = spark.read.load("people.json", format="json")
df_json = spark.read.json("people.json")
df_json.printSchema()

df_json.show()
  
df_json.write.json("newjson_dir")
df_json.write.format("json").save("newjson_dir2")
  
df_json.write.parquet("parquet_dir")
df_json.write.format("parquet").save("parquet_dir2")
  
## Bekerja dengan CSV
csv_df = spark.read.options(header='true',inferSchema='true').csv("cars.csv")

csv_df.printSchema()

csv_df.select('year', 'model').write.options(codec="org.apache.hadoop.io.compress.GzipCodec").csv('newcars.csv')
  
