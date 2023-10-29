// Databricks notebook source
//Creation d'un RDD a patir d'une liste utilisant la transformation parallelize
val rdd = sc.parallelize(List(1,2,3,4,5))
println("Nombre de Partitions: "+rdd.getNumPartitions)
println("Premier element: "+rdd.first())
println("Nombre d'elements: "+rdd.count)

// COMMAND ----------

//action collect
val rddCollect:Array[Int] = rdd.collect()
println("Action: RDD converti en Array[Int] : ")
rddCollect.foreach(println)

// COMMAND ----------

//Collecte par partition
rdd.glom().collect

// COMMAND ----------

//Creation d'un RDD a partir d'un fichier texte
val fileRdd = sc.textFile("./data/file1.txt")
println(fileRdd.first)

// COMMAND ----------

//Creation d'un RDD a partir d'un dossier
val filesRdd = sc.textFile("./data/")
filesRdd.collect

// COMMAND ----------

//Creation d'un RDD a partir d'un dossier
val filesRdd = sc.wholeTextFiles("./data/")
//filesRdd.collect
filesRdd.foreach(f=>println(f._1+ ", " + f._2))

// COMMAND ----------

//Creation d'un RDD a partir d'un fichier CSV
val fileCSVRdd = sc.textFile("./dataCSV/sellers.csv")
println(fileCSVRdd.first)
fileCSVRdd.foreach(println)

// COMMAND ----------


