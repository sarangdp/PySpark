from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import IntegerType

sc = SparkContext(master='local', appName="Getting Created")
spark = SparkSession(sc);

orderCSV = spark.read.csv('C:/Users/Sarang/Documents/hadoop/datasets/retail_db/orders'). \
    toDF('order_id','order_date','order_customer_id','order_status')

orderDF = orderCSV. \
    withColumn('order_id', orderCSV.order_id.cast(IntegerType())). \
    withColumn('order_customer_id', orderCSV.order_customer_id.cast(IntegerType()))

##register the dataframes as temp table

orderDF.registerTempTable("orders")
spark.sql("select * from orders").show()

##Loading the data from Hive

orders = spark.read.table('retail_db.orders').show()
employees = spark.sql("select * from foodmart.employees").show()
