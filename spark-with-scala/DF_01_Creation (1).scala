// Databricks notebook source
import org.apache.spark.sql.SparkSession

// COMMAND ----------

val spark = SparkSession.builder.
        master("local[*]").
        getOrCreate()

// COMMAND ----------

spark.sparkContext.getConf.getAll.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creation d'un DataFrame à partir d'un RDD

// COMMAND ----------

val df = sc.parallelize(List(1,2,3,4,5)).toDF

// COMMAND ----------

val df = sc.parallelize(List((1,"un"),
                             (2,"deux"),
                             (3,"trois"),
                             (4,"quatre"),
                             (5,"cinq")
                            )
                       ).toDF

// COMMAND ----------

// MAGIC %md
// MAGIC ### Création d'un DataFrame à partir d'une liste

// COMMAND ----------

val employees = List((1, "Scott", "Tiger", 1000.0, "united states"),
                     (2, "Henry", "Ford", 1250.0, "India"),
                     (3, "Nick", "Junior", 750.0, "united KINGDOM"),
                     (4, "Bill", "Gomes", 1500.0, "AUSTRALIA")
                    ).toDF

// COMMAND ----------

// MAGIC %md
// MAGIC ### Creation d'un DataFrame à partir d'un fichier
// MAGIC
// MAGIC Passons en revue les API de lecture Spark pour lire des fichiers de différents formats.
// MAGIC
// MAGIC
// MAGIC * `spark` à un ensemble d'API pour lire des données à partir de fichiers de différents formats.
// MAGIC * Toutes les API sont exposées sous `spark.read`
// MAGIC  * `text` - pour lire les données d'une seule colonne à partir de fichiers texte.
// MAGIC  * `csv`- pour lire des fichiers texte avec des délimiteurs. La valeur par défaut est une virgule, mais nous pouvons également utiliser d'autres délimiteurs.
// MAGIC  * `json` - pour lire les données des fichiers JSON.
// MAGIC  * `orc` - pour lire les données des fichiers ORC.
// MAGIC  * `parquet` - pour lire les données des fichiers Parquet.
// MAGIC  * Nous pouvons également lire des données à partir d'autres formats de fichiers en utilisant `spark.read.format`
// MAGIC * Nous pouvons également passer des options basées sur les formats de fichiers. Accédez à la documentation pour obtenir la liste des options disponibles.
// MAGIC  * `inferSchema` - pour déduire les types de données des colonnes en fonction des données.
// MAGIC  * `header` - pour utiliser l'en-tête pour obtenir les noms de colonne dans le cas de fichiers texte.
// MAGIC  * `schema` - pour spécifier explicitement le schéma.
// MAGIC * Voyons un exemple sur la façon de lire des données délimitées à partir de fichiers texte.

// COMMAND ----------

// MAGIC %fs ls /FileStore/tables

// COMMAND ----------

// spark.read.csv
val orders = spark.
    read.
    schema("""order_id INT, order_date TIMESTAMP,
              order_customer_id INT, order_status STRING
           """).
    csv("/FileStore/tables/part_00000.csv")

// COMMAND ----------

// spark.read.csv with option

val orders = spark.
    read.
    schema("""order_id INT, order_date TIMESTAMP,
              order_customer_id INT, order_status STRING
           """).
    option("sep", ",").
    csv("/FileStore/tables/part_00000.csv")

// COMMAND ----------

//spark.read.csv
val sellers = spark.read.csv("./dataCSV/sellers.csv")
sellers.show

// COMMAND ----------

//spark.read.csv avec options
val sellers =spark.
read.
//schema(""" seller_id INT, seller_name STRING, daily_target INT """).
option("inferSchema", true).
option("header", true).
option("sep", ",").
csv("./dataCSV/sellers.csv")

// COMMAND ----------

// spark.read.format

val orders = spark.
    read.
    schema("""order_id INT, order_date TIMESTAMP,
              order_customer_id INT, order_status STRING
           """).
    option("sep", ",").
    format("csv").
    load("./files/retail_db/orders")

// COMMAND ----------

// MAGIC %md
// MAGIC * Lecture de données JSON à partir de fichiers texte. Nous pouvons déduire le schéma à partir des données car chaque objet JSON contient à la fois le nom et la valeur de la colonne.
// MAGIC * Exemple pour JSON
// MAGIC
// MAGIC ```
// MAGIC { "order_id": 1, "order_date": "2013-07-25 00:00:00.0", "order_customer_id": 12345, "order_status": "COMPLETE" }
// MAGIC ```

// COMMAND ----------

// spark.read.json

val orders = spark.
    read.
    option("inferSchema", "false").
    schema("""order_id INT, order_date TIMESTAMP,
              order_customer_id INT, order_status STRING
           """).
    json("./files/retail_db_json/orders")

// COMMAND ----------

// spark.read.format

val orders = spark.
    read.
    option("inferSchema", "false").
    schema("""order_id INT, order_date TIMESTAMP,
              order_customer_id INT, order_status STRING
           """).
    format("json").
    load("./files/retail_db_json/orders")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Aperçu du schéma et des données
// MAGIC
// MAGIC Voici les API qui peuvent être utilisées pour prévisualiser le schéma et les données.
// MAGIC
// MAGIC * `printSchema` peut être utilisé pour obtenir les détails du schéma.
// MAGIC * `show` peut être utilisé pour prévisualiser les données. Il affichera généralement les 20 premiers enregistrements où la sortie est tronquée.
// MAGIC * `describe` peut être utilisé pour obtenir des statistiques à partir de nos données.
// MAGIC * Nous pouvons transmettre le nombre d'enregistrements et définir truncate sur false lors de la prévisualisation des données.
// MAGIC

// COMMAND ----------

val orders = spark.
    read.
    schema("""order_id INT, 
              order_date STRING, 
              order_customer_id INT, 
              order_status STRING
           """
          ).
    csv("./files/retail_db/orders")

// COMMAND ----------

orders

// COMMAND ----------

// Print Schema
orders.printSchema

// COMMAND ----------

// Describe
orders.describe().show(false)

// COMMAND ----------

// Preview Data - Default
orders.show

// COMMAND ----------

// Preview Data - 10, with truncate false
orders.show(10, truncate=false)

// COMMAND ----------


