# Insight Data Engineering Project
## Project idea
This project is proposed to show real-time traffic flow volume and compare to historical traffic volume at a certain time of a specific location, as well as preliminarily analyze the correlation between traffic flow volume and multiple factors based on real-time traffic direction and volume count from traffic observation stations of Department of Transportation.
## Project purpose
Increased amount of vehicles will potentially bring more problems such as traffic congestion, incidents and air pollution. The purpose of this project is to build a data pipeline that support a web application which can show both historical traffic flow volume and monitor real-time traffic, and the correlation between traffic flow volume with road type, lane number change, truck ratio, etc. The traffic flow volume data will be transformed, stored and served the web application, which is shown as a heatmap.
## Use cases
The web application allows users to track the historical traffic flow volume on any observation station on a specific time or the changes within a specific time period.<br>
The correlation between traffic flow volume and multiple factors can be used to improve road condition and/or adjust toll/carpool policy to reduce potential traffic congestion.<br>
Generate an alert if the local traffic is significantly more than usual.
## Main challenge
Retrieving historical traffic volume, calculating statistic information and comparing to real-time traffic volume.<br>
Finding correlation between traffic flow volume and multiple factors using classification and regression.<br>
Combining the data with geolocation.
## Data source
The data will be the traffic volume count (number of vehicles towards every possible direction) at hourly bases from traffic observation stations of Department of Transportation. Data size is about ~2Gb per year in past years but can randomly generate more similar data for earlier years without records or simulated observation stations.<br>
## Technologies to be used
### File storage
S3: more elastic, less expensive, availability and durability
### Batch processing
Spark: more mature and more third-party libraries
### Real-time streaming
Kafka: simple to use<br>
Spark Streaming: support MapReduce<br>
PostgreSQL: geolocation analysis
## Proposed architecture
![image](https://raw.githubusercontent.com/YIZHUSTC/InsightDE/master/architecture.png)
