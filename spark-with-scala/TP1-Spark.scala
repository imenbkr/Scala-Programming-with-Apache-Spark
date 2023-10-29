// Databricks notebook source
// MAGIC %scala
// MAGIC import pyspark

// COMMAND ----------

val myList = List(50,6,54,9,325,47,62)

// COMMAND ----------

val myRDD = sc.parallelize(myList)
//parallelize : transforms data to rdd
//sc : spark context
// another method : get file from external resources
// or : from an existant RDD

// COMMAND ----------

val result1 = myRDD.reduce(_+_)
// val result1 = myRDD.sum

// COMMAND ----------

val even_nums = myRDD.filter(_ % 2 ==0) 

// COMMAND ----------

val odd_nums = myRDD.filter(_ % 2 !=0) 

// COMMAND ----------

val carree = myRDD.map(x=>x*x)

// COMMAND ----------

//even_nums.foreach(println())
carree.collect()

// COMMAND ----------

myRDD.getNumPartitions

// COMMAND ----------

val myRDD2 = sc.textFile("/FileStore/tables/test-4.txt")

// COMMAND ----------

myRDD2.count()

// COMMAND ----------

val result = myRDD2.flatMap(x=>x.trim.split(" "))
result.collect.foreach(println) //collect => transforms data into arrays in memory

// COMMAND ----------

result.count()

// COMMAND ----------

val result1 = result.map(word => (word,1)).reduceByKey(_+_)

// COMMAND ----------

result1.collect.foreach(println)

// COMMAND ----------

result1.saveAsTextFile("/FileStore/tables/result1.txt")

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables

// COMMAND ----------

//Exercice 3

val myRDD3 = sc.textFile("dbfs:/FileStore/tables/bus.txt")

// COMMAND ----------

myRDD3.collect.foreach(println)

// COMMAND ----------

myRDD3.count()

// COMMAND ----------

val busMap= myRDD3.
  map(x => x.split(","))

// COMMAND ----------

//(Reason)
val busTuple = busMap.map(fields => (fields(4), 1))

// COMMAND ----------

val busTupleGroupBy = busTuple.reduceByKey(_ + _)

// COMMAND ----------

busTupleGroupBy.collect()
      .sortBy(-_._2) // Sort by count in descending order
      .foreach { case (reason, count) =>
        println(s"Reason: $reason, Count: $count")
      }

// COMMAND ----------

//(Run_Type, Route_Number)
val busTuple1 = busMap.map(fields => (fields(1), fields(3)))

// COMMAND ----------

val routeCounts = busTuple1
  .map { case (_, routeNumber) => (routeNumber, 1) }
  .reduceByKey(_ + _)

// COMMAND ----------

val top5Routes = routeCounts
  .takeOrdered(5)(Ordering[Int].reverse.on { case (_, count) => count })

// COMMAND ----------

top5Routes.foreach(println)

// COMMAND ----------

//(Occured_On , Number_Of_Students_On_The_Bus)
val busDateAndStudents = busMap
      .map(fields => (fields(5), fields(6).toInt))
    

// COMMAND ----------

val busYearlyGroups = busDateAndStudents
      .map { case (date, students) => (date.split("-")(0), students) }
      .groupByKey()

// COMMAND ----------

val annualAvgStudents = busYearlyGroups
      .mapValues(students => students.sum / students.size)

// COMMAND ----------

val result = annualAvgStudents.collect()
    result.foreach { case (year, avg) =>
      println(s"Year: $year, Annual Average Students on Bus: $avg")
    }

// COMMAND ----------


