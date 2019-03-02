from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import IntegerType, FloatType

sc = SparkContext(master='local', appName="Getting Created")
spark = SparkSession(sc);

orderCSV = spark.read.csv('C:/Users/Sarang/Documents/hadoop/datasets/retail_db/orders'). \
    toDF('order_id','order_date','order_customer_id','order_status')

orderDF = orderCSV. \
    withColumn('order_id', orderCSV.order_id.cast(IntegerType())). \
    withColumn('order_customer_id', orderCSV.order_customer_id.cast(IntegerType()))
orderDF.printSchema()


orderItemsCSV = spark.read.csv('C:/Users/Sarang/Documents/hadoop/datasets/retail_db/order_items'). \
    toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',
         'order_item_quantity', 'order_item_subtotal','order_item_product_price')

orderItemsDF = orderItemsCSV. \
    withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
    withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
    withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
    withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
    withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
    withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))

orderItemsDF.printSchema()
