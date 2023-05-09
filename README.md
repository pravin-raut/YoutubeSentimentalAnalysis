# Sentiment Analysis of YouTube Comments using Kafka
This project demonstrates how to perform sentiment analysis of YouTube comments using Kafka. The architecture of the project involves extracting comments from a YouTube video using the YouTube API, sending the comments to Kafka, consuming them in Databricks, performing sentiment analysis using a Python package like TextBlob or VADER, and storing the results back in Kafka for further processing.

Prerequisites

To run this project, you will need:

1) A Google Cloud account to access the YouTube API and create a VM instance on Google Cloud.
2) A Kafka installation to act as the message broker between the YouTube API and Databricks.
3) A Databricks account to consume the comments from Kafka, perform sentiment analysis, and store the results back in Kafka.

