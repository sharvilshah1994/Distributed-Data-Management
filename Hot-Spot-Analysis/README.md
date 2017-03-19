# Hot Spot Analysis (Getis-Ord Gi*) (Spatial Statistics)
This project is an implementation of ACM's 5th Annual GIS-focused algorithm competition (**ACM-SIGSPATIAL-GISCUP-2016**).The Problem Definition page is here: http://sigspatial2016.sigspatial.org/giscup2016/problem.

## Motto
With the advent of the ubiquitous collection of spatio-temporal observational data (e.g., vehicle tracking data), the identification of unusual patterns of data in a statistically significant manner has become a key problem that numerous businesses and organizations are attempting to solve.

## Input
1. Subset of New York City Taxi and Limousine Commission Yellow Cab trip data.
2. Around 1 Billion Records (e.g., latitude 40.5N – 40.9N, longitude 73.7W – 74.25W).
3. Each record in the dataset contains key information such as pickup and dropoff date, time, and location (latitude, longitude), trip distance, passenger count, and fare amount.
4. The dataset is/must be stored in HDFS as a collection of uncompressed CSV files.

## Task
Identification of 50 most statistically significant dropoff locations by passenger count in both time and space using the Getis-Ord statistic along with the utilization of Apache Spark open source cluster computing framework.

## Procedure
1. Organize given data into a 3D Cube Model where X: Latitude, Y: Longitude & Z: Day. Here X ~ [40.5N to 40.9N], Y ~ [-73.7W to -74.25W] & Z ~ [January 1 to January 31].
2. Find Z-score of each cell.
3. Extract cells with top 50 Z-scores.

## Assumptions
1. Units of X & Y: 0.0001 Degrees.
2. Units of Z: 1 day.

## Output
A JAR file with all dependencies which would be called like as shown below from apache spark. This would generate a csv with top 50 hotspots listed.
```
./spark-submit --class com.dds.Hot_Spot.Hotspots /path_to/Hot_Spot.jar "/path_to/input.csv" "/path_to/output.csv"
```
## Folder Description
1. input.csv: The Input file.
2. Hot_Spot.jar: The Output jar.
3. Hotspots_result.csv: Top 50 hotspots extracted by the Hot_Spot.jar from input.csv.
4. Source: Folder containing source code.
