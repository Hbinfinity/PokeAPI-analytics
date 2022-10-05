# PokeAPI-analytics
Analytics on Public Pokemon Data available on PokeAPI 

1. Running on Docker Container.

docker run -it -p 8888:8888 -v %cd%:/home/jovyan/work/projects/ jupyter/pyspark-notebook:lab-3.4.7

2. Install Python libraries further with the requirements.txt.

3. Install Delta.

pyspark --packages io.delta:delta-core_2.12:2.1.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

