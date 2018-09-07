# TrafficAdvisor: a Real-Time Traffic Monitoring System
## Insight Data Engineering Project
## Project idea
This project is designed to present real-time traffic flow volume on the map and allow user to query current, historical average and predicted traffic volume and other road information from the nearest traffic sensor on any designated geolocation.
## Project purpose
Increased amount of vehicles will potentially bring more problems such as traffic congestion, incidents and air pollution. The purpose of this project is to build a data pipeline that support a web application shows the amount of real-time traffic volume on map, and other information upon request, including current and historical average traffic volume, the prediction of future traffic volume and static information of the sensor and road.
## Use cases
Allows users to retrieve historical average traffic volume on the nearest traffic sensor based on the geolocation on a specific time or the changes within a specific time period.<br>
The prediction of future traffic volume based on current traffic flow volume pattern and other factors can be used for adaptive toll fare/carpool system on congested roads to reduce potential traffic congestion, taxi fare prediction, navigation and providing suggestions for urban planners.<br>
Generate alerts if the local traffic is significantly higher than normal.
## Main challenges
Retrieving historical traffic volume, calculating statistic information and comparing it with real-time traffic volume.<br>
Finding correlation between traffic flow volume and multiple factors using machine learning.<br>
Combining data with latitude and longitude, and allow geolocation query of users.
## Data source
The data will be the traffic volume count (number of vehicles towards every possible direction) at hourly bases from traffic observation sensors of Department of Transportation. Data size is about ~200 Gb per year in past years but can randomly generate data following the same pattern for earlier years without records or simulated observation stations, and for future real-time traffic data.<br>
## Technologies used
### File storage
S3: more elastic, less expensive, availability and durability
### Batch processing
Spark: more mature and more third-party libraries, Spark MLlib is used for modeling
### Real-time streaming
Kafka: ingest data and produce tral-time data<br>
Spark Streaming: consume microbatches from Kafka and does real-time analysis
### Database
PostgreSQL + PostGIS extention: store data with geolocation for analysis/presentation/user query
### Front end
Flask + Leaflet: map presentation
## Architecture
![image](https://raw.githubusercontent.com/YIZHUSTC/InsightDE/master/architecture.png)
The infrastructure consists of both batch processing and streaming processing. The historical data is stored in Amazon S3. In batch processing, Spark does the aggregation, filtration and profiling for the baseline of traffic volume patterns , and a linear regression model is trained for each sensor based on various features such as location, road type, traffic direction and traffic volume pattern within past 24 hours. The processed data is stored in PostgreSQL, and the regression model is stored back in S3. While in streaming processing, the simulated real-time data is ingested by Kafka, and consumed by Spark Streaming. Spark Streaming compares current data with historical data, and takes the trained model to predict traffic volume for next hour. The real-time data with geolocations is also maintained in PostgreSQL. And finally Flask is used to response to the user query and present the real-time traffic volume on the map.
## Demo
No live demo since EC2 instances were terminated!<br>
[![Watch the video](https://lh3.googleusercontent.com/Ned_Tu_ge6GgJZ_lIO_5mieIEmjDpq9kfgD05wapmvzcInvT4qQMxhxq_hEazf8ZsqA=s180-rw)](https://www.youtube-nocookie.com/embed/PudAhKbvdb0)
