from diagrams import Diagram, Cluster, Edge
from diagrams.gcp.compute import GPU
from diagrams.onprem.analytics import Databricks
from diagrams.onprem.compute import Server
from diagrams.onprem.queue import Kafka
from diagrams.custom import Custom

with Diagram("YouTube Comment Sentiment Analysis Architecture", show=False):
    with Cluster("Google Cloud Platform"):
        youtube_api = Custom("YouTube API", "youtube.jpg")

    with Cluster("VM Instance") :
        kafka = Kafka("Kafka")

    with Cluster("Data Processing"):
            databricks = Databricks("Databricks")

    youtube_api >> Edge(label="Reads") >> kafka
    kafka >> Edge(label="Consumes") << databricks




