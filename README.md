# Insight Data Engineering Project
## Project idea
This project is proposed to retrieve historical traffic volume and preliminarily estimate the traffic flow volume in a coming time of a specific location, based on real-time traffic direction and volume count from 28k traffic observation stations of Department of Transportation.
## Project purpose
Increased amount of vehicles will potentially bring more problems such as traffic congestion, incidents and air pollution. The purpose of this project is to build a data pipeline that support a web application which can show both historical traffic flow volume and monitor real-time traffic, as well as the impact of future traffic to a specific area. The real-time observed traffic flow volume data will be transformed, stored and served the web application, which is shown as a heatmap.
## Use cases
The web application allows users to track the historical traffic flow volume on any observation station on a specific time or the changes within a specific time period.<br>
The impact of traffic flow volume of a specific location (longitude and latitude) in the following minutes or hour can be estimated and shown as a heatmap, based on the relative geolocation of nearby observation stations.<br>
Generate an alert if the local traffic is significantly more than usual.
## Main challenge
Retrieving historical traffic volume, calculating statistic information and comparing to real-time traffic volume.<br>
Combining the data with geolocation.
## Data source
The data will be the traffic volume count (number of vehicles towards every possible direction) at hourly bases from 28k traffic observation stations of Department of Transportation. Data size is about ~10Gb per year in past years but can randomly generate more similar data for earlier years without records or simulated observation stations.<br>
## Technologies to be used
### File storage
S3: more elastic, less expensive, availability and durability
### Batch processing
Spark: more mature and more third-party libraries
### Real-time streaming
Kafka: simple to use<br>
Spark Streaming: support MapReduce<br>
Cassandra: column-oriented, good for time-series data
## Proposed architecture
![image](https://raw.githubusercontent.com/YIZHUSTC/InsightDE/master/arch.jpg)
