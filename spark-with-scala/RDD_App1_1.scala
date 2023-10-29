// Databricks notebook source
// MAGIC %md
// MAGIC # Get details of orders
// MAGIC
// MAGIC Details - Duration 15 to 20 minutes
// MAGIC
// MAGIC Data is available in local file system /files/retail_db  \
// MAGIC Source directories: /data/retail_db/orders  \
// MAGIC Source delimiter: comma (“,”) \
// MAGIC Source Columns - orders - **order_id, order_date, order_customer_id, order_status** \
// MAGIC
// MAGIC - Get the total number of orders
// MAGIC - Get the total number of orders by **status**
// MAGIC - Get the orders with a status **not complete**, sorted by **order_date**  \
// MAGIC - Get the total number of orders by **order_customer_id** \
// MAGIC
// MAGIC

// COMMAND ----------

import org.apache.spark.sql.SparkSession
val sc = SparkSession.builder().
    master("local[*]").
    getOrCreate().
    sparkContext

//master : manager of resources 
//local : on the local machine 
// 2 : number of cores , if * : access to the cores available 

// COMMAND ----------

val orders = sc.textFile("/FileStore/tables/part_00000.csv")

//read textfile

// COMMAND ----------

// MAGIC %md
// MAGIC ### Get the total number of orders

// COMMAND ----------

val totalOrders = orders.count

//action

//transformation : applies a function/transformation on a RDD => result : RDD1
//action : result : anything other than a RDD

// COMMAND ----------

// MAGIC %md
// MAGIC ### Get the number of orders by status

// COMMAND ----------

val ordersMap = orders.
  map(order => order.split(","))

// => A, B, C => [A, B, C] => ..

// COMMAND ----------

val ordersTuple = ordersMap.map(f=> (f(0), f(1), f(2), f(3)))

//(A, B, C) 

// COMMAND ----------

val ordersTupleGroupBy = ordersTuple.groupBy(f=>f._4).map(f=>(f._1, f._2.size)).
collect.foreach(println)

// how to access elements in a tuple : (A, B, C) => ._1 => A , ._2 => B

//(1, .., .., A) => (A, [(1, ..,..), (......), (....)]) (key , list of all elements that have the key in it in the previous tuple)
//(2, .., .., B) => (B, [(2, ..,..), (......), (....)])
//(3, .., .., C) => (C, [(1, ..,..), (......), (....) ])


//map the second RDD to find the first element and size of the list

// COMMAND ----------

ordersMap.map(f=>(f(3),1)).reduceByKey(_+_).collect.foreach(println)

//(1, .., .., A) => (A, 1) => (A, ..) count all the occurences of each key and add them to 1
//(2, .., .., B) => (B, 1) => (B, ..)
//(3, .., .., C) => (C, 1) => (C, ..)

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

ordersTuple.keyBy(f=>f._4).countByKey

//(1, .., .., A) => (A, (1, .., ., A)) => count by key 
//(2, .., .., B) => (B, (2, .., .., B))
//(3, .., .., C) => (C, (3, .., .., C))

// COMMAND ----------

val totalOrdersByStatus = ordersMap.groupByKey(f => f(3))
    .map(f => (f._1, f._2.size))
    .sortByKey()

totalOrdersByStatus.collect.foreach(println)

// COMMAND ----------

val totalOrdersByStatus = ordersMap.map(f=>(f(3), f))
    .groupByKey()
    .map(f=>(f._1, f._2.size))
    .sortByKey()

totalOrdersByStatus.collect.foreach(println)

// COMMAND ----------

val numberByStatus = ordersMap.keyBy(f => f(3)).countByKey().toList.sorted.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Get the orders with a status not complete, sorted by order_date

// COMMAND ----------

val ordersMapFiltered = ordersMap.filter(f => f(3).equals("COMPLETE"))
    .sortBy(f => f(1)).map(f => f(0)+","+f(1)+","+f(2)+","+f(3))

ordersMapFiltered.take(10).foreach(println)

// COMMAND ----------

val ordersMapFiltered = ordersMap.map(f => (f(0), f(1), f(2), f(3))).filter(f => f._4.equals("COMPLETE"))
    .sortBy(f => f._2)

ordersMapFiltered.take(10).foreach(println)

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables/

// COMMAND ----------


