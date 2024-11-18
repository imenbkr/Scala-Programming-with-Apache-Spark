// Databricks notebook source
// MAGIC %md
// MAGIC ## Exercice 1 

// COMMAND ----------

// chargement du fichier "bus.txt" dans un RDD
val bus = spark.sparkContext.textFile("/FileStore/bus.txt")

// COMMAND ----------

// 1.	Les raisons les plus courantes d’un retard ou d’une panne d’autobus
val reasonsCount = bus.map(line => line.split(",")(4)).countByValue()
val topReasons = reasonsCount.toList.sortBy(-_._2).take(5)

// COMMAND ----------

// 2.	Les cinq principaux numéros d’itinéraire où l’autobus a été retardé ou en panne
val routeCount = bus.map(line => (line.split(",")(3), 1)).reduceByKey(_ + _)
val topRoutes = routeCount.sortBy(-_._2).take(5)
println("Les cinq principaux numéros d'itinéraire où l'autobus a été retardé ou en panne :")
topRoutes.foreach(println)

// COMMAND ----------

// 3.	Le nombre total d’incidents, lorsque les élèves étaient dans l’autobus
val incidentsWithStudents = bus.filter(line => line.split(",")(6).toInt > 0)
val totalIncidentsWithStudents = incidentsWithStudents.count()
print("total Incidents With Students: ", totalIncidentsWithStudents)

// COMMAND ----------

// 4.	Le nombre total d’incidents, lorsque les élèves n’étaient pas dans l’autobus
val incidentsWithNoStudents = bus.filter(line => line.split(",")(6).toInt == 0)
val totalIncidentsWithNoStudents = incidentsWithNoStudents.count()


// COMMAND ----------

// 5.	L’année où les retards ont été moins nombreux.
val yearRDD = bus.map(line => line.split(",")(0))
val yearCountsRDD = yearRDD.map(year => (year, 1)).reduceByKey(_ + _)
val minYearDelays = yearCountsRDD.min()(Ordering.by(_._2))
println(s"The year with the least delays was ${minYearDelays._1} with ${minYearDelays._2} delays.")


// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercice 2

// COMMAND ----------

// chargement de "crime.csv" dans un RDD
val crimeData = sc.textFile("/FileStore/chicago_crime_2016-1.csv")
val header = crimeData.first
print(header)
val crimesRDD = crimeData.filter(criminalRecord => criminalRecord != header)
crimesRDD.take(5).foreach(println)

// COMMAND ----------

// 1. Identification des types de crimes les plus courants
val crimeTypeCounts = crimesRDD.map(crime => (crime.split(",")(5), 1)).reduceByKey(_ + _).sortBy(_._2, false).take(5)
crimeTypeCounts.foreach(println)

// COMMAND ----------

// 2.	Analyse des tendances des différents types de crimes au fil des mois
val monthlyCrimeCounts = crimesRDD.map(crime => {
  val month = crime.split(",")(2).split(" ")(0).split("/")(0)
  (month, 1)
})
.reduceByKey(_ + _)
.sortByKey().collect()
monthlyCrimeCounts.foreach(println)

// COMMAND ----------

// 3. les trois principaux endroits (îlots ou secteurs) où les taux de crimes sont les plus élevés.
val locationCrimeCounts = crimesRDD.map(crime => (crime.split(",")(7), 1)).reduceByKey(_ + _).sortBy(_._2, false).take(3)
locationCrimeCounts.foreach(println)

// COMMAND ----------

// 4. les trois principaux types de crimes en fonction du nombre d’incidents dans la zone est « RESIDENCE » en utilisant « Location_Description»
// Filter the RDD to include only RESIDENCE area crimes
val residenceCrimesRDD = crimesRDD.filter(crime => crime.split(",")(7).toUpperCase == "RESIDENCE")

// Map crime types with count of incidents
val crimeTypeCounts = residenceCrimesRDD.map(crime => (crime.split(",")(5), 1)).reduceByKey(_ + _).sortBy(_._2, false).take(3)
crimeTypeCounts.foreach(println)

// COMMAND ----------

// 5. le pourcentage de crimes qui mènent à une arrestation.
val totalCrimes = crimesRDD.count()
val arrestedCrimes = crimesRDD.filter(crime => crime.split(",")(8) == "true").count()
val arrestRate = (arrestedCrimes.toDouble / totalCrimes.toDouble) * 100


// COMMAND ----------

// 6. les jours de la semaine où les taux de crimes sont les plus élevés et les plus faibles.
val dayOfWeekCrimeCounts = crimesRDD.map(crime => {
  val dayOfWeek = crime.split(",")(2).split(" ")(0)
  (dayOfWeek, 1)}).reduceByKey(_ + _)
val higherdayOfWeekCrime= dayOfWeekCrimeCounts.sortBy(_._2,false).take(1)
val lowerdayOfWeekCrime= dayOfWeekCrimeCounts.sortBy(_._2,true).take(1)

// COMMAND ----------

// 7. le pourcentage d’affaires domestiques pour différents types de crimes.
val totalCrimes = crimesRDD.count()
val domesticCrimes = crimesRDD.filter(crime => crime.split(",")(9) == "true").count()
val nonDomesticCrimes = totalCrimes - domesticCrimes
val domesticPercentage = (domesticCrimes.toDouble / totalCrimes.toDouble) * 100

// COMMAND ----------

// 8.	Enquêter si certains types de crimes est plus courants au cours de saisons ou de mois précis.
val monthlyCrimeCounts = crimesRDD.map(crime => {
  val month = crime.split(",")(2).split(" ")(0).split("/")(0).toInt
  (month, 1)}).reduceByKey(_ + _).sortByKey()
monthlyCrimeCounts.sortBy(_._2, false).take(1)
