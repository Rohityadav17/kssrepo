from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Load CSV into DataFrame
df = spark.read.csv("C:/kss/BigDataProject/data/sample_data.csv", header=True, inferSchema=True)

# Show data
df.show()

print('------------------------------------------------------------------------------->')

# Select specific columns
df.select("name", "salary").show()

# Filter rows where age is greater than 30
df.filter(df.age > 30).show()

spark.stop()