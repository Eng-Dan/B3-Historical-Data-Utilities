# Code for use in Jupyter Notebook
# The dataframe schema is applied as per B3 "SeriesHistórica_Layout.pdf" instructions available in "http://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/cotacoes-historicas/"


pip install pyspark

from  pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

import pyspark.sql.functions as F
import pyspark.sql.types as T
import re

# Helper function
def test_regexp(stringData, expression, insensitiveFlag):
    if insensitiveFlag:
        pattern = re.compile(r"" + expression, re.I)
    else:
        pattern = re.compile(r"" + expression)

    if re.search(pattern, stringData):
            return True
    return False

# set functions as a pyspark UDF
udf_test_regexp = F.udf(lambda stringData, expression, insensitiveFlag: test_regexp(stringData, expression, insensitiveFlag), returnType='boolean')


# Function
def apply_b3_schema(rawDataFrame):
    '''Apply a data frame schema for the B3 Time Series Stock Data.
    param: rawDataFrame: the dataframe created from the raw data file.
    Returns a new pyspark DataFrame'''
    newDF = rawDataFrame\
                        .withColumn('header_and_trail_test', udf_test_regexp(F.col('value'), F.lit('(COTAHIST)'), F.lit(True)))\
                        .filter(F.col('header_and_trail_test') == False)\
                        .withColumn('tipreg', F.substring(F.col('value'), 1, 2).cast(T.IntegerType()))\
                        .withColumn('datpre', F.to_date(\
                                                        F.concat(F.substring(F.col('value'), 3, 4),
                                                                F.lit("-"),
                                                                F.substring(F.col('value'), 7, 2),
                                                                F.lit("-"),
                                                                F.substring(F.col('value'), 9, 2)),
                                                        'yyyy-MM-dd'))\
                        .withColumn('codbdi', F.substring(F.col('value'), 11, 1))\
                        .withColumn('codneg', F.trim(F.substring(F.col('value'), 13, 12)))\
                        .withColumn('tpmerc', F.substring(F.col('value'), 25, 3).cast(T.IntegerType()))\
                        .withColumn('nomres', F.trim(F.substring(F.col('value'), 28, 12)))\
                        .withColumn('especi', F.substring(F.col('value'), 40, 10))\
                        .withColumn('prazot', F.trim(F.substring(F.col('value'), 50, 3)))\
                        .withColumn('modref', F.trim(F.substring(F.col('value'), 53, 4)))\
                        .withColumn('preabe', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 57, 11),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 68, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(13, 2)))\
                        .withColumn('premax', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 70, 11),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 81, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(13, 2)))\
                        .withColumn('premin', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 83, 11),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 94, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(13, 2)))\
                        .withColumn('premed', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 96, 11),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 107, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(13, 2)))\
                        .withColumn('preult', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 109, 11),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 120, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(13, 2)))\
                        .withColumn('preofc', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 122, 11),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 133, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(13, 2)))\
                        .withColumn('preofv', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 135, 11),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 146, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(13, 2)))\
                        .withColumn('totneg', F.substring(F.col('value'), 148, 5).cast(T.IntegerType()))\
                        .withColumn('quatot', F.substring(F.col('value'), 153, 18).cast(T.IntegerType()))\
                        .withColumn('voltot', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 171, 16),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 187, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(18, 2)))\
                        .withColumn('preexe', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 189, 11),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 200, 2)),
                                                            r"^[0]*", "").cast(T.DecimalType(13, 2)))\
                        .withColumn('indopc', F.substring(F.col('value'), 202, 1).cast(T.IntegerType()))\
                        .withColumn('datven', F.to_date(\
                                                        F.concat(F.substring(F.col('value'), 203, 4),
                                                                F.lit("-"),
                                                                F.substring(F.col('value'), 207, 2),
                                                                F.lit("-"),
                                                                F.substring(F.col('value'), 209, 2)),
                                                        'yyyy-MM-dd'))\
                        .withColumn('fatcot', F.substring(F.col('value'), 211, 7).cast(T.IntegerType()))\
                        .withColumn('ptoexe', F.regexp_replace(\
                                                            F.concat(\
                                                                        F.substring(F.col('value'), 218, 1),
                                                                        F.lit("."),
                                                                        F.substring(F.col('value'), 219, 6)),
                                                            r"^[0]*", "").cast(T.DecimalType(7, 6)))\
                        .withColumn('codisi', F.substring(F.col('value'), 231, 12))\
                        .withColumn('dismes', F.substring(F.col('value'), 243, 3).cast(T.IntegerType()))\
                        .drop('')

    newDF = newDF.drop('value', 'header_and_trail_test')

    return newDF


# Path to the time series raw data file dowloaded
FILE = '.../COTAHIST.AAAA.TXT'

b3TimeSeriesDF = spark.read.text(FILE)

# Function call and schema applycation
b3TimeSeriesDF = apply_b3_schema(b3TimeSeriesDF)
b3TimeSeriesDF.show()
