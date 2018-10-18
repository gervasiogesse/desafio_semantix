#--------------------------------------------------------------------
# Script criado por Gervasio Gesse Junior em 17/Out/2018
# desafio vaga semantix
#--------------------------------------------------------------------

# Verifica se existe a variável de ambiente do SPARK_HOME
# Se não existir carrega o caminho do SPARK_HOME
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/opt/spark")
}

library(SparkR)

# caminho dos arquivos
caminho <- "./arqs"

# # inicia o sparkSession
# sparkR.session(appName = "Desfio_Semantix",
#                sparkPackages = "com.databricks:spark-csv_2.11:1.0.3")

# sparkR.session(appName = "Desfio_Semantix",
#                sparkPackages = "org.apache.spark:spark-hive_2.11:2.3.0")

sparkR.session(appName = "Desfio_Semantix")

# Define o schema dos arquivos
stringSchema <- "host STRING, c2 STRING, c3 STRING, c4 STRING, c5 STRING, c6 STRING, c7 INT, c8 INT"


# Cria o spark dataframe
df <- read.df(caminho, source = "csv", header = "false", schema = stringSchema, 
              inferSchema = "false", na.strings = "NA", delimiter = " "
              )

#head(df)
# sql

# Registra o SparkDataFrame como uma view temporaria.
createOrReplaceTempView(df, "df")

# para realizar o parse da data são necessarios 3 Ms um para cada letra do mes

# teste <- sql("SELECT host, concat(c2,c3) as teste, to_timestamp(regexp_extract(concat(c4,c5),'.(.+).'),'dd/MMM/yyyy:HH:mm:ss-zzzz') as time FROM df")
# teste <- sql("SELECT host, concat(c2,c3) as teste, to_timestamp('01/Aug/1995:00:00:00-0400', 'dd/MMM/yyyy:HH:mm:sszzzzz') as time FROM df")
logs <- sql("SELECT host, concat(c2,c3) as teste, 
             to_timestamp(concat(c4,c5),'[dd/MMM/yyyy:HH:mm:sszzzzz]') as timestamp,
             c6 as request, c7 as codigo, c8 as bytes
             FROM df")
# confere o schema
printSchema(logs)

head(logs)

# Registra o SparkDataFrame como uma view temporaria.
createOrReplaceTempView(logs, "logs")



print("Número de hosts únicos e total de bytes retornados:")
saida <- sql("SELECT count(DISTINCT host) AS Quantidade_hosts, sum(bytes) AS Total_Bytes 
             FROM logs")
head(saida)

print("O total de erros 404:")
saida <- sql("SELECT count(*) AS Erros_404 
             FROM logs 
             WHERE codigo == 404")
head(saida)

print("Os 5 URLs que mais causaram erro 404")
urls_erros <- sql("SELECT count(request) AS quantidade,request 
                  FROM logs 
                  WHERE codigo == 404 
                  GROUP BY request ORDER BY quantidade DESC")
head(urls_erros,num=5L)

print("Quantidade de erros 404 por dia:")
urls_erros <- sql("SELECT date_format(timestamp, 'y-MM-dd') AS Dia, count(*) AS Quantidade 
                  FROM logs 
                  WHERE codigo == 404 
                  GROUP BY Dia 
                  ORDER BY quantidade DESC")
head(urls_erros, num = 60L)
