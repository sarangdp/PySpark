from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, FloatType
from pyspark.sql.functions import sum, round

orderCSV = spark.read.csv('/user/sarangdp/sqoop_import/retail_db/orders'). \
toDF('order_id','order_date','order_customer_id','order_status')

orderDF = orderCSV. \
withColumn('order_id', orderCSV.order_id.cast(IntegerType())). \
withColumn('order_customer_id', orderCSV.order_customer_id.cast(IntegerType()))

orderItemsCSV = spark.read.csv('/user/sarangdp/sqoop_import/retail_db/order_items'). \
toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',
'order_item_quantity', 'order_item_subtotal','order_item_product_price')

 orderItemsDF = orderItemsCSV. \
withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))

##Window function
from pyspark.sql.window import Window
spec = Window.partitionBy(orderItemsDF.order_item_order_id)
type(spec) ## <class 'pyspark.sql.window.WindowSpec'>

##using over function and spec

 orderItemsDF \
 .select('order_item_order_id','order_item_subtotal',round(orderItemsDF.order_item_subtotal/sum(orderItemsDF.order_item_subtotal) \
 .over(spec),2).alias('order_rev')).show()
