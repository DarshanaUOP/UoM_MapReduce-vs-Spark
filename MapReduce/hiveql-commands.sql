-- create a database 
create database aircraftdelay;

-- use previously created database 
use aircraftdelay;

-- create a table to match each columns in the csv file 
CREATE EXTERNAL TABLE delayedflights (rowID bigint, Year int, Month int, DayofMonth int, DayOfWeek int, DepTime double, CRSDepTime int, ArrTime double, CRSArrTime int, UniqueCarrier string, FlightNum int, TailNum String, ActualElapsedTime double, CRSElapsedTime double, AirTime double, ArrDelay double, DepDelay double, Origin String, Dest String, Distance int, TaxiIn double, TaxiOut double, Cancelled int, CancellationCode String, Diverted int, CarrierDelay double, WeatherDelay double, NASDelay double, SecurityDelay double, LateAircraftDelay double ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE TBLPROPERTIES ('skip.header.line.count'='1');

-- read csv file from s3 bucket and insert it into the table
load data inpath 's3://dar-flight-delay-demo/input/DelayedFlights-updated.csv' into table delayedflights ;

-- calculate total_carrier_delay
SELECT Year , SUM(CarrierDelay) AS total_carrier_delay FROM delayedflights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year;

-- calculate total_nas_delay
SELECT Year , SUM(NASDelay) AS total_nas_delay FROM delayedflights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year;

-- calculate total_weather_delay
SELECT Year , SUM(WeatherDelay) AS total_weather_delay FROM delayedflights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year;

-- calculate total_late_a_ircraft_delay
SELECT Year , SUM(LateAircraftDelay) AS total_late_a_ircraft_delay FROM delayedflights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year;

-- calculate total_security_delay
SELECT Year , SUM(SecurityDelay) AS total_security_delay FROM delayedflights WHERE Year >= 2003 AND Year <= 2010 GROUP BY Year;