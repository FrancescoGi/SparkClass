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

    #Show dello schema della tabella
    df_iris.printSchema()


    #ordinato per sepal_length_in_cm in modo ascendente (dal valore più basso al più alto)
    df_iris.orderBy(col("sepal_length_in_cm").asc()).show()
    #ordinato per sepal_length_in_cm in modo discendete (dal valore più alto al più basso)
    df_iris.orderBy(col("sepal_length_in_cm").desc()).show()
    #ordinato per sepal_length_in_cm  e sepal_width_in_cm in modo discendete (dal valore più alto al più basso)
    df_iris.orderBy(col("sepal_length_in_cm").desc(),col("sepal_width_in_cm").desc()).show()

    #ordinato per sepal_length_in_cm in modo discendete (dal valore più alto al più basso)
    #sort() è più efficiente di orderBy() perchè i dati sono ordinati per ogni partizione individualmente
    df_iris.sort(col("sepal_length_in_cm").desc()).show()

    #Selezione di tutte le colonne del DataFrame e aggiunta di un campo calcolato summary (di tipo stringa) mediante diverse logiche
    case_when_df = df_iris.select(df_iris['*'],when(col("sepal_length_in_cm")>5.5,"3").when((col("sepal_length_in_cm")>=4.5) & (col("sepal_length_in_cm")<=5.5),'2' ).otherwise("1").alias("summary"))
    case_when_df.show()
    case_when_df.printSchema()

    # Selezione delle sole colonne class e summary, alla cui colonna summary viene applicato un cast
    # per portarlo da string a intero e al termine viene ridenominato con int_summary.
    # Infine viene fatta un raggruppamento per int_summary e class conteggiando il numero di tutti i valori per ogni valore di int_summary
    case_when_df.select(col("class"), col("sepal_length_in_cm"), col("summary").cast('integer').alias("int_summary")).groupBy(col("int_summary"),col("class")).agg(count("sepal_length_in_cm").alias("count")).show()

    #Aggiunta order by per il campo class e count
    case_when_df.select(col("class"), col("sepal_length_in_cm"), col("summary").cast('integer').alias("int_summary")).groupBy(col("int_summary"),col("class")).agg(count("sepal_length_in_cm").alias("count")).sort(col('class').desc(),col('count').asc()).show()
