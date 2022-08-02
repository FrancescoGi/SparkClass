from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DecimalType, StringType
from pyspark.sql.functions import col, when, lit

if __name__=='__main__':

    #Creazione della sessione spark
    spark = SparkSession.builder.appName("Spark Test").getOrCreate()

    #Definizione dello schema del file CSV
    schema = StructType([
        StructField("sepal_length_in_cm",DecimalType(2,1)),
        StructField("sepal_width_in_cm",DecimalType(2,1)),
        StructField("petal_length_in_cm",DecimalType(2,1)),
        StructField("petal_width_in_cm",DecimalType(2,1)),
        StructField("class",StringType())
    ])

    #Creazione del Data Frame mediante le funzioni spark
    df = spark.read.options(delimiter=',').csv("/Users/francescogiancaspro/Desktop/iris.csv", header=True, schema=schema)

    #Show delle prime righe del DataFrame
    df.show()

    #Print dello schema della tabella
    df.printSchema()

    #Selezione di 3 differenti colonne con 3 differenti notazioni
    df_select = df.select(col("class"), df.sepal_length_in_cm, df["sepal_width_in_cm"])

    df.show()
    df_select.show()

    #Select con operazioni di rinominazione e conversione da cm a mm
    alias_df = df_select.select(col("class").alias("type"),(df.sepal_length_in_cm * 10).alias("sepal_length_in_mm"),col("sepal_width_in_cm"))
    alias_df.show()

    #Selezione di tutte le colonne del DF precedente e aggiunta di un campo calcolato summary mediante diverse logiche
    case_when_df = alias_df.select(alias_df['*'],when(col("sepal_length_in_mm")>55,'BIG').when((col("sepal_length_in_mm")>=45) & (col("sepal_length_in_mm")<=55),'MIDIUM').otherwise("SMALL").alias("summary"))
    case_when_df.show()

    #Selezione di tutte le colonne del DF precedente e aggiunta di una colonna con un valore costante
    const_df = case_when_df.select(case_when_df['*']).withColumn("constant", lit("FLOWER"))
    const_df.show()

    #Selezione di tutte le colonne del DF precedente e cast del campo sepal_length_in_mm da decimal a integer
    cast_df = const_df.select(const_df['*'],col("sepal_length_in_mm").cast("integer").alias("int_value"))
    cast_df.show()
    cast_df.printSchema()