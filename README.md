# Sentiment Analysis of YouTube Comments using Kafka
This project demonstrates how to perform sentiment analysis of YouTube comments using Kafka. The architecture of the project involves extracting comments from a YouTube video using the YouTube API, sending the comments to Kafka, consuming them in Databricks, performing sentiment analysis using a Python package like TextBlob or VADER, and storing the results back in Kafka for further processing.

## Prerequisites

1) A Google Cloud account to access the YouTube API and create a VM instance on Google Cloud.
2) A Kafka installation to act as the message broker between the YouTube API and Databricks.
3) A Databricks account to consume the comments from Kafka, perform sentiment analysis, and store the results back in Kafka.

## Architecture
![Youtube Sentimental Analysis](https://user-images.githubusercontent.com/65663124/237044334-01b7b4c5-d03b-47e5-93d1-f7062371b674.png)

## Preview

https://user-images.githubusercontent.com/65663124/237046885-ce83ee7b-c0c4-4ae2-bccc-937d52897ec9.mp4

