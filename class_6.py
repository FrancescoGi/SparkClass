from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DecimalType, StringType
from pyspark.sql.functions import col, when, max, min, avg, sum, count

if __name__=='__main__':
    #Creazione di una sessione Spark
    spark = SparkSession.builder.appName("Spark Test").getOrCreate()

    #Definizione dello schema del file in CSV
    schema = StructType([
        StructField("sepal_length_in_cm",DecimalType(2,1)),
        StructField("sepal_width_in_cm",DecimalType(2,1)),
        StructField("petal_length_in_cm",DecimalType(2,1)),
        StructField("petal_width_in_cm",DecimalType(2,1)),
        StructField("class",StringType())
    ])

    #Creazione del Data Frame mediante le funzioni Spark
    df_iris = spark.read.options(delimiter=',').csv("/Users/francescogiancaspro/Desktop/iris.csv", header=True, schema=schema)

    #Show delle prime righe del DataFrame
    df_iris.show()
    
    df_iris = df_iris.groupBy(col("class")).agg(sum(col("sepal_length_in_cm")))
    df_iris.show()

    #Creazione della lista di tuple dai cui verrà creato il DataFrame
    list = [("Iris-setosa", "Descrizione 1", "Europa"),
            ("Iris-versicolor", "Descrizione 2", "Europa"),
            ("Iris-virginica", "Descrizione 3", "Asia"),
            ("Iris-Test", "Descrizione 3", "Asia")
            ]

    #Definizione dei nomi delle colonne del DataFrame
    column = ["nome","descrizione","continente_dove_cresce"]

    #Creazione del Data Frame mediante le funzioni spark
    df_desc = spark.createDataFrame(data=list, schema=column)

    # Show delle prime righe del DataFrame
    df_desc.show()

    # Print dello schema della tabella
    df_desc.printSchema()

    #df iris viene messo in inner join con df_desc
    #df_iris.join(df_desc, df_iris["class"] == df_desc["nome"], "inner").show()
    #Può dare errori di ambiguità, per cui ha senso usare il dataframe per indicare la colonna

    #df iris viene messo in left join con df_desc
    df_iris.join(df_desc, df_iris["class"] == df_desc["nome"], "left").show()

    #df iris viene messo in right join con df_desc
    df_iris.join(df_desc, df_iris["class"] == df_desc["nome"], "right").show()

    #df iris viene messo in full outer join con df_desc
    df_iris.join(df_desc, df_iris["class"] == df_desc["nome"], "fullouter").show()


    #df iris viene messo in full outer join con df_desc, successivamnete vengono filtrati solo quelli con continente_dove_cresce uguale ad Europa ed infine selezionate solo due colonne (class e descecrizione)
    df_iris.join(df_desc, df_iris["class"] == df_desc["nome"], "fullouter").filter(col("continente_dove_cresce") == 'Europa').select(col("class"), col("descrizione")).show()


    #Esempi di istruzioni SQL (almeno un paio diversi)
    #df_iris.createOrReplaceTempView("temp_view_iris")
    #spark.sql("SELECT * FROM temp_view_iris WHERE sepal_length_in_cm  > 5.0").show()

    #CONTROLLARE: RITORNA TUTTI ANCHE QUELLI CON FULLNAME NON VALORIZZATO..
    #-->FAre una sql con tutti gli operatori visti su spark
    #spark.sql("SELECT class, count(*) FROM temp_view_iris WHERE sepal_length_in_cm  > 5.0 GROUP BY class ORDER BY class asc").show()


    #JOIN IN SQL
