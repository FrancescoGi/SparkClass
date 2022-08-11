from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DecimalType, StringType
from pyspark.sql.functions import col, when, lit

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
    df = spark.read.options(delimiter=',').csv("/Users/francescogiancaspro/Desktop/iris.csv", header=True, schema=schema)

    #Show delle prime righe del DataFrame
    df.show()

    #Show dello schema della tabella
    df.printSchema()

    #Filtrare solo ed esclusivamente le rige con class ugale a Iris-setosa
    df_filter = df.filter(col("class") == "Iris-setosa")
    df_filter.show()

    #Selezione delle sole colonne class e sepal_length_in_cm e successivo filtraggio delle sole rige con class ugale a Iris-setosa
    df.select(col("class"), col("sepal_length_in_cm")).filter(col("class") == "Iris-setosa").show()

    #Filtrare solo ed esclusivamente le rige con class ugale a Iris-setosa
    #e sepal_length_in_cm maggiore di 5.5
    df.filter((col("class") == "Iris-setosa") & (col("sepal_length_in_cm") > 5.5)).show()

    #Selezione della sole colonne class (alias type) e sepal_length_in_cm con successivo
    #filtraggio delle sole rige con class ugale a Iris-setosa e sepal_length_in_cm maggiore di 5.5
    df.select(col("class").alias("type"), col("sepal_length_in_cm")).filter((col("type") == "Iris-setosa") & (col("sepal_length_in_cm") > 5.5)).show()

    #ATTENZIONE:
    #Eseguendo l'oerazione di filter prima di quella di select, lui non riconosce l'alias type dato a class
    #in qunto viene eseguita solo successivamente
    #df.filter((col("type") == "Iris-setosa") & (col("sepal_length_in_cm") > 5.5)).select(col("class").alias("type"), col("sepal_length_in_cm")).show()
    #Ma posso eseguire prima la filter e poi select basta che non uso la colonna ridenominata
    df.filter((col("class") == "Iris-setosa") & (col("sepal_length_in_cm") > 5.5)).select(col("class").alias("type"), col("sepal_length_in_cm")).show()
    #Per cui è possibile eseguire una prima dell'altra e viceversa, ma bisogna fare attenzione a questi dettagli.

    #Selezione di tutte le colonne del DataFrame e aggiunta di un campo calcolato summary (di tipo stringa) mediante diverse logiche
    case_when_df = df.select(df['*'],when(col("sepal_length_in_cm")>5.5,"3").when((col("sepal_length_in_cm")>=4.5) & (col("sepal_length_in_cm")<=5.5),'2' ).otherwise("1").alias("summary"))
    case_when_df.show()
    case_when_df.printSchema()

    #Selezione delle sole colonne class, sepal_length_in_cm e summary, alla cui colonna summary viene applicato un cast
    #per portarlo da string a intero e al termine viene ridenominato con int_summary.
    #Successivamente viene applicato un filtro per selezionare i soli record con class ugale a Iris-setosa e int_summary uguale 3.
    case_when_df = case_when_df.select(col("class"), col("sepal_length_in_cm"), col("summary").cast('integer').alias("int_summary")).filter((col("class") == "Iris-setosa") & (col("int_summary") == 3))
    case_when_df.show()
    case_when_df.printSchema()
