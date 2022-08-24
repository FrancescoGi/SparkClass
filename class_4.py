from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DecimalType, StringType
#Importare max, min e avg altrimenti userebbe quelli built-in di Python
from pyspark.sql.functions import col, min, max, avg, count, sum,  when

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

    #Aggregazione sull'intero dataset, ovvero senza usare alcuna colonna per il raggruppamento. Max di max_sepal_length_in_cm tra tutti i record
    df.agg(max("sepal_length_in_cm").alias("max_sepal_length_in_cm")).show()

    #Raggruppamento sulla colonna class e per ogni valore distinto di class trova l'elemento massimo in sepal_length_in_cm.
    df.groupBy(col("class")).max("sepal_length_in_cm").show()

    #Raggruppamento sulla colonna class ed un campo calcolato. Per ogni valore distinto di class fa la somma di tutti i valori di sepal_length_in_cm.
    df.groupBy(col("class"),when(col("sepal_length_in_cm")>5.5,"BIG").otherwise("SMALL").alias("summary")).sum("sepal_length_in_cm").show()

    #Raggruppamento sulla colonna class e per ogni valore distinto di class trova l'elemento massimo in sepal_length_in_cm (utilizzo della funziona agg() per utilizzare più operatori di aggregazione).
    df.groupBy(col("class")).agg(max("sepal_length_in_cm").alias("max_sepal_length_in_cm")).show()

    #Raggruppamento sulla colonna class e per ogni valore distinto di class ed le principali funzioni di aggregazione.
    df.groupBy(col("class")).agg(sum("sepal_length_in_cm").alias("sum_sepal_length_in_cm"), max("sepal_length_in_cm").alias("max_sepal_length_in_cm"), min("sepal_length_in_cm").alias("min_sepal_length_in_cm"), avg("sepal_length_in_cm").alias("avg_sepal_length_in_cm"), count("sepal_length_in_cm").alias("count_zsepal_length_in_cm")).show()

    #Selezione di tutte le colonne del DataFrame e aggiunta di un campo calcolato summary (di tipo stringa) mediante diverse logiche
    case_when_df = df.select(df['*'],when(col("sepal_length_in_cm")>5.5,"3").when((col("sepal_length_in_cm")>=4.5) & (col("sepal_length_in_cm")<=5.5),'2' ).otherwise("1").alias("summary"))
    case_when_df.show()
    case_when_df.printSchema()

    #Aggregazione con distinct simile ad una group by su tutti i campi
    print(case_when_df.count())
    print(case_when_df.distinct().count())

    # Selezione delle sole colonne class e summary, alla cui colonna summary viene applicato un cast
    # per portarlo da string a intero e al termine viene ridenominato con int_summary.
    # Infine viene fatta un raggruppamento per int_summary e class conteggiando il numero di tutti i valori per ogni valore di int_summary
    case_when_df.select(col("class"), col("sepal_length_in_cm"), col("summary").cast('integer').alias("int_summary")).groupBy(col("int_summary"),col("class")).agg(count("sepal_length_in_cm").alias("count")).show()

    #Selezione delle sole colonne class e summary, alla cui colonna summary viene applicato un cast
    #per portarlo da string a intero e al termine viene ridenominato con int_summary.
    #Successivamente viene applicato un filtro per selezionare i soli record con class ugale a Iris-setosa.
    #Infine viene fatta un raggruppamento per int_summary conteggiando il numero di tutti i valori per ogni valore di int_summary
    case_when_df.select(col("class"), col("sepal_length_in_cm"), col("summary").cast('integer').alias("int_summary")).filter(col("class") == 'Iris-setosa').groupBy(col("int_summary")).agg(count("sepal_length_in_cm").alias("count")).show()
