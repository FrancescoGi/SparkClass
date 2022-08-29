# This is a sample Python script.

# Press Maiusc+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max, col
from pyspark.sql.types import StructType, IntegerType, DateType, StringType, StructField, DecimalType
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
import matplotlib.pyplot as plt
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator

#Sessione di SparkSQL
def sparkFromPythonArray(spark):
    lista_python = [("Gianni","Neri","maschio", 35),
                    ("Bianca", "Marini", "femmina", 30),
                    ("Rossella", "Bianchi", "femmina", 29),
                    ("Giuseppe", "Manzi", "maschio", 31),
                    ("Giuseppe", "Manzi", "maschio", 40)]

    colonne = ["nome","cognome","sesso","eta"]

    #Creo dataFrame da lista Pyton
    df = spark.createDataFrame(data=lista_python, schema=colonne)
    df.show()
    df.printSchema()
    #-->Perchè lo casti se già è un long? Alla fine cambia solo la lunghezza del numero che puoi esprimere
    df.select(df.eta).printSchema()

    #Raggruppamento per eta.. o altra op. di raggruppamento?
    #-->Se usi un raggruppamento, per far vedere al prof che funziona l'aggregazione dovresti mettere almeno persone omonime con età diverse
    #-->per dare un senso all'aggregazione aggregerei per sesso e prendendo il max età e la count, dato che nella traccia
    #-->parla di operazioni di aggregazione
    #-->Con il costrutto che hai usato tu puoi fare solo una max, perchè se mettessi in coda altro verebbero falsati i valori
    df.groupBy("sesso").agg(max("eta").alias("max"), min("eta").alias("min")).show()

def sparkFromCsvFile(spark):
    #Creo dataframe da file cvs
    #-->Conviene sempre fare il cast dei tipi a monte in modo che tutte le operazioni vengano fatte sui tipi corretti
    #-->e non devono esserci cast di default o ulteriori cast definiti da te, infatti non potresti fare aggregazioni min e max
    schema = StructType([
        StructField("day", StringType()),
        StructField("task", StringType()),
        StructField("duration", IntegerType())
    ])

    df2 = spark.read.csv("agenda.csv",header=True, schema=schema)
    df2.show()
    df2.printSchema()
    #Selezione di colonne e trasformo in intero il campo "duration"
    df2.select(df2.task, df2.duration).printSchema()

    #Raggruppo per task e sommo le duration - NON FUNZIONANO:
    #-->La risposta è quella descritta sopra, non puoi fare una somma sulle stringhe.
    #-->Il cast, l'hai fatto solo a livello di show
    # "duration" colonna non numerica? cast sbagliato?
    df2.groupBy("task").sum("duration").show()
    #'DataFrame' object has no attribute 'max' ?
    #-->Non puoi fare una max senza un operatore di aggregazione
    df2.agg(max("duration").alias("max")).show() #-->Ma non credo sia quello che voglia tu, ti tira il max su tutto il set di dati così
    df2.groupBy("day").agg(max("duration").alias("max")).show() #-->Ma non credo sia quello che voglia tu

def sparkFromJsonFile(spark):
    #Creazione DF da file json multiriga
    df3 = spark.read.option("multiline","true").json("prova.json")
    df3.show()
    df3.printSchema()
    #Selezione di colonne e rinomina sesta colonna
    #-->Usare notazione con col invece che ad indice, perchè più leggibile
    df3.select(col("Competition"),col("IsCaptain").alias("Capitano")).printSchema()
    #Ordinamento per Club
    #(p.s. esiste anche orderBy di spark, inoltre definire se è desc o asc, di default è sempre asc
    df3.sort(col("Club").desc()).show()
    #Filtro i risultati per cui la colonna Team corrisponde al valore Argentina
    df3.filter(col("Team") == "Argentina").show()

    #Raggruppamento per Country e torno il max valore per Anno
    df3.groupBy(df3.ClubCountry).max("Year").show()

    #Esempi di istruzioni SQL (almeno un paio diversi)
    df3.createOrReplaceTempView("temp_view")
    spark.sql("SELECT * FROM temp_view WHERE Year BETWEEN 2000 AND 2015").show()

    #CONTROLLARE: RITORNA TUTTI ANCHE QUELLI CON FULLNAME NON VALORIZZATO..
    #-->FAre una sql con tutti gli operatori visti su spark
    spark.sql("SELECT team, count(*) FROM temp_view WHERE FullName IS NOT NULL GROUP BY team ORDER BY team asc").show()

def regressione(spark):
    schema = StructType([
        StructField("fixed acidity", DecimalType(10,6)),
        StructField("volatile acidity", DecimalType(10,6)),
        StructField("citric acid", DecimalType(10,6)),
        StructField("residual sugar", DecimalType(10,6)),
        StructField("chlorides", DecimalType(10,6)),
        StructField("free sulfur dioxide", DecimalType(10,6)),
        StructField("total sulfur dioxide", DecimalType(10,6)),
        StructField("density", DecimalType(10,6)),
        StructField("pH", DecimalType(10,6)),
        StructField("sulphates", DecimalType(10,6)),
        StructField("alcohol", DecimalType(38,2)),
        StructField("quality", DecimalType(10,6))
    ])

    training = spark.read.options(delimiter=';').csv("winequality-original.csv",header=True, schema=schema)
    training = training.na.drop()
    training.show()
    training.printSchema()

    va = VectorAssembler(inputCols=training.columns, outputCol="features")
    adj = va.transform(training)
    adj.show(3)

    lab = adj.select("features", "quality")
    training_data = lab.withColumnRenamed("quality", "label")
    training_data.show(3)

    lr = LinearRegression(maxIter=30, regParam=0.3, elasticNetParam=0.3, featuresCol="features", labelCol="label")

    lrModel = lr.fit(training_data)
    predictionsDF = lrModel.transform(training_data)
    predictionsDF.show()

    # Print the coefficients and intercept for linear regression
    print("Coefficients: %s" % str(lrModel.coefficients))
    print("Intercept: %s" % str(lrModel.intercept))

    # Summarize the model over the training set and print out some metrics
    trainingSummary = lrModel.summary
    print("numIterations: %d" % trainingSummary.totalIterations)
    print("objectiveHistory: %s" % str(trainingSummary.objectiveHistory))
    trainingSummary.residuals.show()
    print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
    print("r2: %f" % trainingSummary.r2)

def decisiontree(spark):
    schema = StructType([
        StructField("fixed acidity", DecimalType(10, 6)),
        StructField("volatile acidity", DecimalType(10, 6)),
        StructField("citric acid", DecimalType(10, 6)),
        StructField("residual sugar", DecimalType(10, 6)),
        StructField("chlorides", DecimalType(10, 6)),
        StructField("free sulfur dioxide", DecimalType(10, 6)),
        StructField("total sulfur dioxide", DecimalType(10, 6)),
        StructField("density", DecimalType(10, 6)),
        StructField("pH", DecimalType(10, 6)),
        StructField("sulphates", DecimalType(10, 6)),
        StructField("alcohol", DecimalType(38, 2)),
        StructField("quality", DecimalType(10, 6))
    ])

    training = spark.read.options(delimiter=';').csv("winequality-original.csv", header=True, schema=schema)
    data = training.na.drop()
    data.show()
    data.printSchema()
    # Create an assembler object

    va = VectorAssembler(inputCols=data.columns, outputCol="features")
    adj = va.transform(data)
    adj.show(3)

    # Check the resulting column
    adj = adj.select('features', adj.quality.alias('label'))
    adj.show(3)
    train, test = adj.randomSplit([0.8, 0.2], seed=17)

    # Check that training set has around 80% of records
    training_ratio = train.count() / adj.count()
    print(training_ratio)
    """
    # Create a classifier object and fit to the training data
    tree = DecisionTreeRegressor()
    tree_model = tree.fit(train)

    # Create predictions for the testing data and take a look at the predictions
    prediction = tree_model.transform(test)
    prediction.show()
    """
    # Train a DecisionTree model.
    dt = DecisionTreeRegressor()
    # Chain indexer and tree in a Pipeline
    pipeline = Pipeline(stages=[dt])
    # Train model. This also runs the indexer.
    model = pipeline.fit(train)
    # Make predictions.
    predictions = model.transform(test)
    # Select example rows to display.
    predictions.select("prediction", "label", "features").show(5)
    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
    treeModel = model.stages[0]
    # summary only
    print(treeModel)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Applicazione di test").getOrCreate()
    #sparkFromPythonArray(spark)
    #sparkFromCsvFile(spark)
    #sparkFromJsonFile(spark)
    #regressione(spark)
    decisiontree(spark)
