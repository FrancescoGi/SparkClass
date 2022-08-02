from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DecimalType, StringType


def fromFile(spark):
    """
    Descrizione: Creazione di un DataFrame da un file CSV
    :param spark: sessione
    :return: None
    """
    #Definizione dello schema del file CSV
    schema = StructType([
        StructField("sepal_length_in_cm",DecimalType(2,1)),
        StructField("sepal_width_in_cm",DecimalType(2,1)),
        StructField("petal_length_in_cm", DecimalType(2, 1)),
        StructField("petal_width_in_cm", DecimalType(2, 1)),
        StructField("class", StringType()) #Setosa, Versicolour, Virginica
    ])

    #Creazione del Data Frame mediante le funzioni spark
    df = spark.read.options(delimiter=',').csv("/Users/francescogiancaspro/Desktop/iris.csv", header=True, schema=schema)

    #Show delle prime righe del DataFrame
    df.show()

    #Print dello schema della tabella
    df.printSchema()

def fromArray(spark):
    """
    Descrizione: Creazione di un DataFrame da un file Array
    :param spark: sessione
    :return: None
    """

    #Creazione della lista di tuple dai cui verrà creato il DataFrame
    list = [("Francesco", "M", 34),
            ("Pina", "F", 15),
            ("Giuseppe", "M", 44),
            ("Lucia", "F", 35),
            ("Chiara", "F", 48),
            ("Gianni", "M", 17)]

    #Definizione dei nomi delle colonne del DataFrame
    column = ["name","gender","age"]

    #Creazione del Data Frame mediante le funzioni spark
    df = spark.createDataFrame(data=list, schema=column)

    # Show delle prime righe del DataFrame
    df.show()

    # Print dello schema della tabella
    df.printSchema()

def fromJson(spark):
    """
    Descrizione: Creazione di un DataFrame da un file JSON
    :param spark: sessione
    :return: None
    """

    #Creazione del Data Frame mediante le funzioni spark
    df = spark.read.option("multiline","true").json("/Users/francescogiancaspro/Desktop/dataset.json")

    # Show delle prime righe del DataFrame
    df.show()

    # Print dello schema della tabella
    df.printSchema()

if __name__ == '__main__':
    #Creazione della sessione spark
    spark = SparkSession.builder.appName("Applicazione di test").getOrCreate()

    fromFile(spark)
    fromArray(spark)
    fromJson(spark)
