from pyspark import SparkConf
from pyspark.sql import SparkSession


import os
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
os.environ['PYSPARK_PYTHON'] = 'python3'
spark_master = os.getenv('SPARK_MASTER', 'k8s://https://kubernetes.default.svc.cluster.local')
spark = SparkSession.builder \
    .master(spark_master) \
    .config("spark.driver.maxResultSize", 0) \
    .appName('cached_executor_test') \
    .getOrCreate()
 
sc = spark.sparkContext
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10]).cache()
rdd.saveAsTextFile('spark_test.txt')
