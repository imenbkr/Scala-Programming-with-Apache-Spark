// Databricks notebook source
// MAGIC %md
// MAGIC # Prédiction de désabonnement des clients avec Spark MLlib et Scala
// MAGIC ## I- Introduction
// MAGIC
// MAGIC Dans ce tutoriel, nous allons utiliser Spark MLlib avec Scala pour construire un modèle d’apprentissage automatique pour prédire le taux de désabonnement des clients. L’ensemble de données « Churn_Modelling.csv » contient diverses caractéristiques liées aux clients des banques, et notre objectif est de développer un modèle de classification supervisé pour prédire si un client est susceptible de se désabonner ou non.
// MAGIC ## II- Objectif
// MAGIC L’objectif est de construire un modèle de machine learning capable de prédire si un client est susceptible de se désabonner ou non sur la base de ces fonctionnalités. Le taux de désabonnement, dans ce contexte, fait référence à la probabilité qu’un client quitte la banque. En utilisant les capacités d’apprentissage automatique de Spark MLlib, en particulier pour la classification supervisée, l’objectif est de développer un modèle prédictif qui peut classer avec précision les clients en tant que churners potentiels ou non-churners en fonction des fonctionnalités fournies. 

// COMMAND ----------

// MAGIC %md
// MAGIC ## III- Prétraitement et exploration des données
// MAGIC ### III.1- Importation de bibliothèques et de jeux de données 

// COMMAND ----------

// Importing necessary Spark MLlib libraries
import org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, StandardScaler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Reading the dataset
val data = spark.read.option("header", "true").option("inferSchema","true").csv("/FileStore/Churn_Modelling.csv")


// COMMAND ----------

// MAGIC %md
// MAGIC ### EXPLICATION
// MAGIC **org.apache.spark.ml.feature.{VectorAssembler, StringIndexer, StandardScaler}:**
// MAGIC
// MAGIC *VectorAssembler:* This class is used to transform a set of feature columns into a single feature vector column. It is often used to prepare data for machine learning algorithms that require a single input vector.
// MAGIC
// MAGIC *StringIndexer:* This is used to convert categorical values (string labels) into numerical values. It assigns a unique index to each distinct string in a given column.
// MAGIC
// MAGIC *StandardScaler:* This is a feature transformer that standardizes the features by removing the mean and scaling to unit variance. It helps in normalizing the features, which is important for some machine learning algorithms.
// MAGIC
// MAGIC **org.apache.spark.ml.{Pipeline, PipelineModel}:**
// MAGIC
// MAGIC *Pipeline:* A pipeline chains multiple stages together to specify a sequence of data processing and machine learning tasks. Each stage in the pipeline must be either a Transformer or an Estimator.
// MAGIC *PipelineModel:* Once a pipeline is trained, it produces a PipelineModel, which is essentially a fitted pipeline. The model can be used to make predictions on new data.
// MAGIC
// MAGIC **org.apache.spark.ml.linalg.Vectors:**
// MAGIC
// MAGIC This library provides a set of dense and sparse vector classes. Vectors is often used to represent the input features for machine learning algorithms.
// MAGIC
// MAGIC **org.apache.spark.sql.SparkSession:**
// MAGIC
// MAGIC SparkSession is the entry point to programming Spark with the Dataset and DataFrame API. It is used to create DataFrames, register DataFrames as tables, execute SQL queries, and perform various other operations on structured data.
// MAGIC
// MAGIC **org.apache.spark.sql.functions._:**
// MAGIC
// MAGIC This library provides a set of built-in functions for performing operations on columns in Spark DataFrames. It includes various mathematical, statistical, and string manipulation functions.
// MAGIC
// MAGIC **org.apache.spark.sql.types._:**
// MAGIC
// MAGIC This library provides the data types available in Spark. It is commonly used when defining the schema for DataFrames.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### III.2- description statistique

// COMMAND ----------

//displaying the schema of the dataset
data.printSchema
// Displaying statistical description of the dataset
data.describe().show()


// COMMAND ----------

// 2eme methode pour decrire les données numériques 
//filter les colonnes numériques du dataset, puis faire describe() sur chacune d'eux
val  numcol= data.select(data.columns.filter(colName => data(colName).expr.dataType==IntegerType).map(col):_*)
numcol.show()
for (col<- numcol.columns){
  data.select(col).describe().show()
}

// COMMAND ----------



// COMMAND ----------

// MAGIC %md
// MAGIC ### III.3- Boxplot

// COMMAND ----------

// Displaying boxplot for relevant numerical columns
val boxplotColumns = Seq("CreditScore", "Age", "Tenure", "Balance", "NumOfProducts", "EstimatedSalary", "Exited")
data.select(boxplotColumns.map(col): _*).summary().show()


// COMMAND ----------

//for (col<- boxplotColumns){
  //display(data.select(col))
//}

// COMMAND ----------

// MAGIC %md
// MAGIC ### EXPLICATION
// MAGIC
// MAGIC **val boxplotColumns = Seq("CreditScore", "Age", "Tenure", "Balance", "NumOfProducts", "EstimatedSalary", "Exited"):** This line defines a sequence of column names that you are interested in for the boxplot analysis. These columns presumably contain numerical data that you want to visualize.
// MAGIC
// MAGIC **val selectedData = data.select(boxplotColumns.map(col): _*):** This line selects the specified columns from the dataset (data). It uses the select function, and boxplotColumns.map(col): _* is a way to dynamically pass the column names to the select function.
// MAGIC
// MAGIC **val summaryData = selectedData.summary():** This line calculates summary statistics for the selected columns. The summary() function computes the minimum, 25th percentile (Q1), median (50th percentile), mean, 75th percentile (Q3), and maximum for each specified column.
// MAGIC
// MAGIC **summaryData.show():** Finally, this line displays the calculated summary statistics. The output will include information about count, mean, stddev (standard deviation), min, 25th percentile, median, 75th percentile, and max for each selected column.
// MAGIC
// MAGIC By running this code, you'll get a table with statistical information for the specified numerical columns, providing insights into the central tendency and spread of the data. This helps in understanding the distribution of values and identifying potential outliers.
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ### III.4- variables dépendantes et indépendantes
// MAGIC L’identification et la définition de votre variable dépendante (variable cible à prévoir) et des variables indépendantes (caractéristiques utilisées pour la prédiction) est une étape critique dans toute tâche d’apprentissage automatique. Il constitue la base de la formation modèle.

// COMMAND ----------

// Defining dependent and independent variables
val independentVars = Array("CreditScore", "GeographyIndex", "GenderIndex", "Age", "Tenure", "Balance", "NumOfProducts", "HasCrCard", "IsActiveMember", "EstimatedSalary")
val dependentVar = "Exited"


// COMMAND ----------

// MAGIC %md 
// MAGIC ### III.5- Codage des données catégoriques
// MAGIC Les algorithmes d’apprentissage automatique nécessitent généralement une entrée numérique, de sorte que l’encodage de variables catégoriques est essentiel. Le StringIndexer est utilisé pour convertir des variables catégoriques en format numérique, ce qui les rend appropriés pour la modélisation.

// COMMAND ----------

// Encoding categorical data using StringIndexer
//val indexer1 = new StringIndexer().setInputCol("Geography").setOutputCol("GeographyIndex")
//val indexer2 = new StringIndexer().setInputCol("Gender").setOutputCol("GenderIndex")

//val indexedData = new Pipeline().setStages(Array(indexer1, indexer2)).fit(data).transform(data)


// Creating StringIndexer for "Geography" column
val indexer1 = new StringIndexer().setInputCol("Geography").setOutputCol("GeographyIndex")

// Applying the transformation to add the new column
val data_GeographyIndex = indexer1.fit(data).transform(data)

// Creating StringIndexer for "Gender" column
val indexer2 = new StringIndexer().setInputCol("Gender").setOutputCol("GenderIndex")

// Applying the transformation to add the new column
val data_BothIndices = indexer2.fit(data_GeographyIndex).transform(data_GeographyIndex)

data_BothIndices.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### EXPLICATION
// MAGIC **StringIndexer** is an Estimator in Spark MLlib used to convert categorical strings (labels) into numerical indices. It assigns a unique index to each distinct string in a given column.
// MAGIC In this case, a new instance of StringIndexer is created with the input column set to "Geography" and the output column set to "GeographyIndex". This means it will create a new column named "GeographyIndex" where the unique values in the "Geography" column will be replaced with corresponding numerical indices.
// MAGIC
// MAGIC **fit method:** The fit method is used to compute the StringIndexerModel, which contains the mapping between the original strings and the numerical indices. It is fitted to the data.
// MAGIC
// MAGIC **transform method:** The transform method then applies this model to the data, creating a new DataFrame (data_GeographyIndex) that includes the original columns and the newly created "GeographyIndex" column.

// COMMAND ----------

// MAGIC %md
// MAGIC ### III.6- Splitting de l’ensemble de données 
// MAGIC La division de l’ensemble de données en ensembles de formation et de test est cruciale pour évaluer la performance du modèle. L’ensemble de formation est utilisé pour former le modèle, et l’ensemble d’essais est utilisé pour évaluer dans quelle mesure le modèle se généralise à de nouvelles données invisibles.

// COMMAND ----------

// Splitting the dataset into training and testing sets
val Array(trainData, testData) = data_BothIndices.randomSplit(Array(0.8, 0.2), seed = 1234)
trainData.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC ### EXPLICATION
// MAGIC **seed = 1234** sets the seed for the random number generator to the value 1234.
// MAGIC randomSplit is a method in Spark that is used to randomly split a DataFrame into multiple DataFrames based on the specified weights. In this case, the weights are [0.8, 0.2], meaning that approximately 80% of the data will go into the trainData DataFrame, and 20% will go into the testData DataFrame.
// MAGIC By setting the seed to a specific value, 1234 in this case, the random splitting process will be deterministic, and if you run the code multiple times with the same seed, you will get the same split of data into training and testing sets. This is beneficial for reproducibility, especially in situations where you want to compare model performance consistently across different runs. If you were to omit the seed or use a different seed, you would get different random splits each time you run the code.
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ### III.7- Mise à l’échelle des fonctions (Feature Scaling)
// MAGIC Il est important de normaliser ou de normaliser les caractéristiques, surtout lorsqu’on utilise des algorithmes sensibles à l’échelle des caractéristiques d’entrée (p. ex., des algorithmes basés sur la distance). StandardScaler garantit que toutes les fonctionnalités ont la même échelle.

// COMMAND ----------

// Scaling features using StandardScaler
val assembler = new VectorAssembler().setInputCols(independentVars).setOutputCol("features")
val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(false)

val assembledTrainData = assembler.transform(trainData)
val scaledTrainData = scaler.fit(assembledTrainData).transform(assembledTrainData)

val assembledTestData = assembler.transform(testData)
val scaledTestData = scaler.fit(assembledTestData).transform(assembledTestData)

scaledTrainData.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ### EXPLICATION
// MAGIC **VectorAssembler** is used to assemble the feature columns into a single feature vector column.
// MAGIC
// MAGIC **setInputCols(independentVars)** specifies the input feature columns
// MAGIC
// MAGIC **setOutputCol("features")** sets the name of the output feature vector column to "features".
// MAGIC
// MAGIC **independentVars** presumably contains the names of the feature columns.
// MAGIC
// MAGIC **StandardScaler** is a feature transformer that standardizes features by removing the mean and scaling to unit variance.
// MAGIC
// MAGIC **setInputCol("features")** specifies the input column containing the feature vectors.
// MAGIC
// MAGIC **setOutputCol("scaledFeatures")** sets the name of the output column with the scaled features.
// MAGIC
// MAGIC **setWithStd(true)** indicates that the scaling should be done with the standard deviation.
// MAGIC
// MAGIC **setWithMean(false)** indicates that the mean should not be subtracted before scaling.
// MAGIC
// MAGIC **assembler.transform(trainData)** uses the VectorAssembler to assemble the feature columns in trainData into a new column named "features".
// MAGIC
// MAGIC **scaler.fit(assembledTrainData).transform(assembledTrainData)** applies the StandardScaler to the assembled training data. The fit method is used to compute the scaling parameters (mean and standard deviation), and then the transformation is applied to produce the final scaled training data.

// COMMAND ----------

// MAGIC %md
// MAGIC ## IV- Decision Tree
// MAGIC ### IV.1-  Intuition
// MAGIC Les arbres de décision sont un algorithme d’apprentissage automatique populaire pour les tâches de classification et de régression. L’intuition derrière un arbre de décision est de diviser récursivement l’ensemble de données en sous-ensembles basés sur l’attribut le plus important à chaque nœud. Ce processus se poursuit jusqu’à ce qu’un critère d’arrêt soit respecté, ce qui donne une structure arborescente où les feuilles représentent le résultat prévu.
// MAGIC ### IV.2- classificateur Arbre de désicion

// COMMAND ----------

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// Creating the Decision Tree model
val decisionTree = new DecisionTreeClassifier()
  .setLabelCol("Exited")
  .setFeaturesCol("scaledFeatures") 
  .setImpurity("gini") // You can choose "entropy" or "gini" as the impurity measure
  .setMaxDepth(5) // Set the maximum depth of the tree

// Fitting the model to the training data
val decisionTreeModel = decisionTree.fit(scaledTrainData)

// Making predictions on the test data
val dtPredictions = decisionTreeModel.transform(scaledTestData)

// Evaluating the model
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("Exited")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracyDT = evaluator.evaluate(dtPredictions)
println(s"Accuracy of the Decision Tree model: $accuracyDT")



// COMMAND ----------

// MAGIC %md
// MAGIC ## V- Support Vector Machine
// MAGIC ### V.1- Intuition
// MAGIC Les SVM sont de puissants algorithmes d’apprentissage supervisés utilisés pour les tâches de classification et de régression. L’intuition derrière SVM est de trouver un hyperplan qui sépare le mieux les données en différentes classes. Les "vecteurs de support" sont les points de données les plus proches de l’hyperplan, et l’objectif est de maximiser la marge entre les classes.
// MAGIC ### V.2- Classificateur Support Vector Machine

// COMMAND ----------

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// Creating the Support Vector Machine model
val svm = new LinearSVC()
  .setLabelCol("Exited")
  .setFeaturesCol("scaledFeatures") 

// Fitting the model to the training data
val svmModel = svm.fit(scaledTrainData)

// Making predictions on the test data
val svmPredictions = svmModel.transform(scaledTestData)
svmPredictions.show(5)

// Evaluating the model
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("Exited")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracySVM = evaluator.evaluate(svmPredictions)
println(s"Accuracy of the Support Vector Machine model: $accuracySVM")


// COMMAND ----------

// MAGIC %md
// MAGIC ## VI- Logistic Regression
// MAGIC ### VI.1- Intuition
// MAGIC La régression logistique est un algorithme de classification largement utilisé qui modélise la probabilité d’un résultat binaire. Malgré son nom, il est utilisé pour la classification, pas la régression. L’algorithme applique la fonction logistique à une combinaison linéaire de caractéristiques d’entrée pour obtenir des probabilités, et un seuil est appliqué pour classer les instances dans l’une des deux classes.
// MAGIC ### VI.2- classificateur Logistic Regression

// COMMAND ----------

import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// Creating the Logistic Regression model
val lr = new LogisticRegression()
  .setLabelCol("Exited")
  .setFeaturesCol("scaledFeatures") 

// Fitting the model to the training data
val lrModel = lr.fit(scaledTrainData)

// Making predictions on the test data
val lrPredictions = lrModel.transform(scaledTestData)

// Evaluating the model
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("Exited")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")

val accuracyLR = evaluator.evaluate(lrPredictions)
println(s"Accuracy of the Logistic Regression model: $accuracyLR")


// COMMAND ----------

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._
// Assuming you have accuracy values for each model
val modelNames = Seq("Decision Tree", "SVM", "Logistic Regression")
val accuracyValues = Seq(accuracyDT, accuracySVM, accuracyLR)  // Replace with your actual accuracy values

// Create a DataFrame with model names and accuracy values
val accuracyDF = modelNames.zip(accuracyValues).toDF("Model", "Accuracy")

// Display the DataFrame as a table
accuracyDF.show()



// COMMAND ----------


