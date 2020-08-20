from pyspark.sql import SparkSession


# Instantiate Spark-on-K8s Cluster
spark = SparkSession \
    .builder \
    .appName("Simple Spark Test") \
    .config("spark.executor.memory","8g") \
    .config("spark.executor.cores","2") \
    .config("spark.driver.memory","2g") \
    .config("spark.executor.instances","2") \
  .getOrCreate()

# Validate Spark Connectivity
spark.sql("SHOW databases").show()
spark.sql('use default')
spark.sql('show tables').show()
spark.sql('create table testcml (abc integer)').show()
spark.sql('insert into table testcml select t.* from (select 1) t').show()
spark.sql('select * from testcml').show()
spark.sql('drop table testcml').show()

# Stop Spark Session
spark.stop()


# Run sample HDFS commands
# Requires an additional testdata.txt file to be created with sample data
!hdfs dfs -mkdir /tmp/testcml/
!hdfs dfs -cp /user/admin/testdata.txt /tmp/testcml/
!hdfs dfs -cat /tmp/testcml/testdata.txt
