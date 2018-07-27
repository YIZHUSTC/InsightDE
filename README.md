# Insight Data Engineering Project
## Project idea
This project is proposed to preliminarily estimate the traffic flow volume in the coming hour of a specific location, based on real-time traffic direction and volume count from 28k traffic observation stations of Department of Transportation.
## Project purpose
The purpose of this project is to build a data pipeline that support a web application which can show both historical traffic flow volume and impact of future traffic. The real-time observed traffic flow volume data will be transformed, stored and served the web application.
## Use cases
The web application allows users to track the historical traffic flow volume on any observation station in a specific time.<br>
The impact of traffic flow volume of a specific location (longitude and latitude) in the following minutes or hour can be estimated and shown as a heatmap.
## Data source
The data will be the traffic volume count (number of vehicles towards every possible direction) at hourly bases from 28k traffic observation stations of Department of Transportation. Data size is about ~10Gb per year but can randomly generate more similar data.<br>
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
