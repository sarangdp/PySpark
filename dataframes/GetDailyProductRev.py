##Get product rev

 from pyspark.sql.types import IntegerType, FloatType
 
orderCSV = spark.read.csv('/user/sarangdp/sqoop_import/retail_db/orders'). \
...     toDF('order_id','order_date','order_customer_id','order_status')

 orderDF = orderCSV. \
...     withColumn('order_id', orderCSV.order_id.cast(IntegerType())). \
...     withColumn('order_customer_id', orderCSV.order_customer_id.cast(IntegerType()))

orderItemsCSV = spark.read.csv('/user/sarangdp/sqoop_import/retail_db/order_items'). \
...     toDF('order_item_id', 'order_item_order_id', 'order_item_product_id',
...          'order_item_quantity', 'order_item_subtotal','order_item_product_price')

 orderItemsDF = orderItemsCSV. \
...     withColumn('order_item_id', orderItemsCSV.order_item_id.cast(IntegerType())). \
...     withColumn('order_item_order_id', orderItemsCSV.order_item_order_id.cast(IntegerType())). \
...     withColumn('order_item_product_id', orderItemsCSV.order_item_product_id.cast(IntegerType())). \
...     withColumn('order_item_quantity', orderItemsCSV.order_item_quantity.cast(IntegerType())). \
...     withColumn('order_item_subtotal', orderItemsCSV.order_item_subtotal.cast(FloatType())). \
...     withColumn('order_item_product_price', orderItemsCSV.order_item_product_price.cast(FloatType()))

##Join orderDF and orderItemsDF
## inner join
orderDF.where("order_status in ('COMPLETE','CLOSED')").join(orderItemsDF, orderDF.order_id == orderItemsDF.order_item_order_id).show()

## left outer join- Get orders which doesnt have any records in order items
orderDF.where("order_status in ('COMPLETE','CLOSED')").join(orderItemsDF, orderDF.order_id == orderItemsDF.order_item_order_id, 'left') \
.where(orderItemsDF.order_item_order_id.isNull()).select(orderDF.order_id, orderDF.order_status).show()

## Aggregation
from pyspark.sql.functions import sum as _sum
orderItemsDF.filter("order_item_order_id = 2").agg(_sum("order_item_subtotal")).show()

##Group by
 orderItemsDF.groupBy("order_item_order_id").agg(_round(_sum("order_item_subtotal"),2).alias("order_rev")).show()
