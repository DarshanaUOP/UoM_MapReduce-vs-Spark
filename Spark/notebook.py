import time as t
from pyspark.sql.functions import sum

t.time()

df = spark.read.csv("s3://dar-flight-delay-demo/input/DelayedFlights-updated.csv", header=True, inferSchema=True)
df.count()

# test mechanism for calculate the execution time
start_time = t.time()
df.count()
print(f"Time taken: {(t.time()-start_time):.2f} s")

# total carrier delays from 2003 to 2010
start_time = t.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").agg(sum("CarrierDelay").alias("TotalCarrierDelay")).show()
print(f"Time taken: {(t.time()-start_time):.2f} s")

# total NAS delay from 2003 to 2010
start_time = t.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").agg(sum("NASDelay").alias("TotalNASDelay")).show()
print(f"Time taken: {(t.time()-start_time):.2f} s")

# total Weather delay from 2003 to 2010
start_time = t.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").agg(sum("WeatherDelay").alias("TotalWeatherDelay")).show()
print(f"Time taken: {(t.time()-start_time):.2f} s")


# total Late Aircraft delay from 2003 to 2010
start_time = t.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").agg(sum("LateAircraftDelay").alias("TotalLateAircraftDelay")).show()
print(f"Time taken: {(t.time()-start_time):.2f} s")

# total security delay from 2003 to 2010
start_time = t.time()
process_df = df.filter((df.Year >= 2003) & (df.Year <= 2010)).groupBy("Year").agg(sum("SecurityDelay").alias("TotalSecurityDelay")).show()
print(f"Time taken: {(t.time()-start_time):.2f} s")