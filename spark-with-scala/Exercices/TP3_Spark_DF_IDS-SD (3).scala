// Databricks notebook source
// MAGIC %md ## Exercice 1

// COMMAND ----------

import org.apache.spark.sql.functions._

// Load the CSV file into a DataFrame
val crimesDF = spark.read.option("header", "true").csv("/FileStore/chicago_crime_2016-1.csv")
crimesDF.show(1)

// COMMAND ----------

//1. les cinq principaux types de crimes ayant le plus grand nombre d’affaires dans l’ensemble

// Group by Primary Type and count the number of cases for each type
val topCrimeTypes = crimesDF.groupBy("Primar_yType").agg(count("*").alias("top_five_crime_type")).orderBy($"top_five_crime_type".desc)
// Show the top five crime types
topCrimeTypes.show(5)

// COMMAND ----------

// 2.	Les tendances des différents types de crimes au fil des mois.
// Convert the "Date" column to a date type
val crimesWithMonth = crimesDF.withColumn("Month", month(to_date(col("Date"), "MM/dd/yyyy hh:mm:ss a")))
// Group by Month, and count the number of cases
val crimeTrends = crimesWithMonth.groupBy("Month","Primar_yType").count()

// Show the crime trends
crimeTrends.show()

// COMMAND ----------

//3.	Les trois principaux endroits (îlots ou secteurs) où les taux de crimes sont les plus élevés.
// Group by Block and count the number of cases for each block
val crimeRates = crimesDF.groupBy("Location_Description").count()
//crimeRates.show()
// Calculate the crime rate by dividing the number of cases by the total number of cases
val totalCases=crimesDF.count()
print(totalCases)
val crimeRatesWithPercentage = crimeRates.withColumn("CrimeRate", col("count") * 100 / totalCases)
// Select the top three locations with the highest crime rates
val topThreeLocations = crimeRatesWithPercentage.orderBy($"CrimeRate".desc).limit(3)

topThreeLocations.show()

// COMMAND ----------

// 4.	Les trois principaux types de crimes en fonction du nombre d’incidents dans la zone est « RESIDENCE » en utilisant « Location_Description».
val residenceCrimes = crimesDF.filter(col("Location_Description") === "RESIDENCE")
val topThreeCrimeTypesInResidence = residenceCrimes.groupBy("Primar_yType").agg(count("*").alias("IncidentCount")).orderBy($"IncidentCount".desc).limit(3)
topThreeCrimeTypesInResidence.show()

// COMMAND ----------

// 5.	le pourcentage de crimes qui mènent à une arrestation
// Calculate the total number of crimes
val totalCrimes = crimesDF.count()

// Calculate the number of crimes leading to an arrest
val arrestedCrimes = crimesDF.filter(col("Arrest") === "true").count()

// Calculate the percentage
val arrestPercentage = (arrestedCrimes.toDouble / totalCrimes.toDouble) * 100

println(s"The percentage of crimes leading to an arrest is: $arrestPercentage%")

// COMMAND ----------

//6.	les jours de la semaine où les taux de crimes sont les plus élevés et les plus faibles.
// Convert the "Date" column to a date type
val crimesWithDate = crimesDF.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy hh:mm:ss a"))

// Extract the day of the week from the "Date" column
val crimesWithDayOfWeek = crimesWithDate.withColumn("DayOfWeek", date_format(col("Date"), "EEEE"))

// Group by DayOfWeek and count the number of cases for each day
val crimeRatesByDayOfWeek = crimesWithDayOfWeek.groupBy("DayOfWeek").count()

// Find the day with the highest crime rate
val highestCrimeDay = crimeRatesByDayOfWeek.orderBy($"count".desc).first()

// Find the day with the lowest crime rate
val lowestCrimeDay = crimeRatesByDayOfWeek.orderBy($"count").first()

println(s"The day with the highest crime rate is: ${highestCrimeDay.getString(0)}")
println(s"The day with the lowest crime rate is: ${lowestCrimeDay.getString(0)}")

// COMMAND ----------

// 7.	le pourcentage d’affaires domestiques pour différents types de crimes.
// Calculate the total number of domestic cases
val totalDomesticCases = crimesDF.filter(col("Domestic") === "true").count()

// Group by PrimaryType and calculate the total number of cases for each type
val totalCasesByType = crimesDF.groupBy("Primar_yType").count()

// Calculate the percentage of domestic cases for each type
val domesticPercentageByType = totalCasesByType.withColumn("DomesticPercentage",
  (col("count") * 100 / totalDomesticCases).cast("double"))

domesticPercentageByType.show()

// COMMAND ----------

// 8.	Enquêter si certains types de crimes est plus courants au cours de saisons ou de mois précis.
// Convert the "Date" column to a date type
val crimesWithDate = crimesDF.withColumn("Date", to_date(col("Date"), "MM/dd/yyyy hh:mm:ss a"))

// Extract the month and season from the "Date" column
val crimesWithMonthAndSeason = crimesWithDate
  .withColumn("Month", month(col("Date")))
  .withColumn("Season", expr("CASE WHEN Month IN (12, 1, 2) THEN 'Winter' " +
    "WHEN Month IN (3, 4, 5) THEN 'Spring' " +
    "WHEN Month IN (6, 7, 8) THEN 'Summer' ELSE 'Fall' END"))

// Group by Season, PrimaryType and count the number of cases for each combination
val crimeCountsBySeasonAndType = crimesWithMonthAndSeason
  .groupBy("Season", "Primar_yType")
  .count()

crimeCountsBySeasonAndType.show()

// COMMAND ----------

// MAGIC %md ## Exercice 2

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// 1.
val productsDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/products.csv")
val sellersDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/sellers.csv")
val salesDF = spark.read.option("header", "true").option("inferSchema", "true").csv("/FileStore/sales.csv")


productsDF.show(5)
sellersDF.show(5)
salesDF.show(5)

// COMMAND ----------

//2. 
productsDF.printSchema()
sellersDF.printSchema()
salesDF.printSchema()

// COMMAND ----------

//3. 
val numOrders = salesDF.select("order_id").distinct().count()
val numProducts = productsDF.count()
val numSellers = sellersDF.count()

println(s"Number of orders: $numOrders")
println(s"Number of products: $numProducts")
println(s"Number of sellers: $numSellers")


// COMMAND ----------

// 4. 
val productsSoldAtLeastOnce = salesDF.select("product_id").distinct().count()
println(s"Number of products sold at least once: $productsSoldAtLeastOnce")


// COMMAND ----------

// 5.
val productWithMostSales = salesDF.groupBy("product_id").count().orderBy($"count".desc).limit(1).show()

// COMMAND ----------

//6. 
val productsSoldPerDay = salesDF.groupBy("date").agg(countDistinct("product_id").as("distinct_products")).show()


// COMMAND ----------

//7.
val sellersWhoSold = salesDF.select("seller_id").distinct().count()
println(s"Number of sellers who sold products: $sellersWhoSold")


// COMMAND ----------

//8. 
val totalQuantitySold = salesDF.selectExpr("sum(num_pieces_sold) as total_quantity").show()


// COMMAND ----------

//9.
val quantitySoldPerSeller = salesDF.groupBy("seller_id").agg(sum("num_pieces_sold").as("total_quantity")).show()


// COMMAND ----------

//10.
val piecesSoldBySellerByProduct = salesDF.groupBy("seller_id", "product_id").agg(sum("num_pieces_sold").as("total_pieces_sold")).show()


// COMMAND ----------

//11.
val joinedDF = salesDF.join(productsDF, "product_id")
val revenueDF = joinedDF.withColumn("revenue", col("price") * col("num_pieces_sold"))
val avgOrderRevenue = revenueDF.groupBy("order_id").agg(avg("revenue").as("average_order_revenue"))
avgOrderRevenue.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### questions supplémentaires pour l’exercice 2

// COMMAND ----------

// 1. Déterminer la contribution aux revenus de chaque vendeur : (1st method)
//Rejoignez les ventes et les produits sur product_id, puis rejoignez le résultat avec les vendeurs sur seller_id.
//Calculez la contribution au revenu de chaque vendeur en pourcentage du revenu total.

val joinedDF = salesDF.join(productsDF, salesDF("product_id") === productsDF("product_id")).join(sellersDF, salesDF("seller_id") === sellersDF("seller_id"))withColumn("revenue", salesDF("num_pieces_sold") * productsDF("price"))
val totalRevenue = joinedDF.groupBy(sellersDF("seller_id"), sellersDF("seller_name")).agg(sum(col("revenue")).alias("total_revenue"))
val totalSum = totalRevenue.agg(sum(col("total_revenue")).as("total_sum")).collect()(0).getDouble(0)
val revenueContribution=totalRevenue.withColumn("contribution_percentage", (col("total_revenue") / lit(totalSum)).multiply(100))
totalRevenue.show()
revenueContribution.show()



// COMMAND ----------

// calculer la valeur moyenne des transactions par vendeur :
//Rejoignez les ventes et les produits sur product_id, puis rejoignez le résultat avec les vendeurs sur seller_id.
//Calculez la valeur moyenne des transactions pour chaque vendeur.
val joinedDF2 = salesDF
  .join(productsDF, salesDF("product_id") === productsDF("product_id"))
  .join(sellersDF, salesDF("seller_id") === sellersDF("seller_id"))

val transactionValue = joinedDF2
  .withColumn("transaction_value", salesDF("num_pieces_sold") * productsDF("price"))
  .groupBy(sellersDF("seller_id"), sellersDF("seller_name"))
  .agg(avg(col("transaction_value")).alias("avg_transaction_value"))

val avgTransactionValueBySeller = transactionValue
  .select(sellersDF("seller_name"), col("avg_transaction_value"))

avgTransactionValueBySeller.show()


// COMMAND ----------

//Déterminer la tendance des revenus par vendeur
//Rejoignez les ventes et les vendeurs sur seller_id.
//Grouper par seller_id et date, et analyser la tendance des revenus pour chaque vendeur au fil du temps.
val joinedDF3 = salesDF
  .join(sellersDF, salesDF("seller_id") === sellersDF("seller_id"))
  .join(productsDF, salesDF("product_id") === productsDF("product_id"))

val revenueTrend = joinedDF3
  .withColumn("revenue", salesDF("num_pieces_sold") * lit(productsDF("price")))
  .withColumn("month", month(col("date")))
  .withColumn("year", year(col("date")))
  .groupBy(sellersDF("seller_id"), col("year"), col("month"))
  .agg(sum(col("revenue")).alias("monthly_revenue"))
  .orderBy(sellersDF("seller_id"), col("year"), col("month"))

revenueTrend.show()


// COMMAND ----------

//déterminer la segmentation de la clientèle par achat de produits
//Joignez les ventes et les produits sur product_id.
//Analysez les segments de clientèle en fonction des produits qu’ils achètent.
val joinedDF4 = salesDF
  .join(productsDF, salesDF("product_id") === productsDF("product_id"))

val customerSegmentation = joinedDF4
  .groupBy(salesDF("order_id"))
  .agg(countDistinct(productsDF("product_id")).alias("num_products_purchased"))

customerSegmentation.show()


// COMMAND ----------


