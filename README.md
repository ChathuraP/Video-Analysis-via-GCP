# Video-Analysis-via-GCP
project for Intro to cloud technologies

This data pipeline starts with video inserted into the cloud basket. On upload, a finalize event could be triggered, and video will be sent to the Video Intelligence API. Once it is processed, the result will be stored in another cloud bucket. At the same time, the result data will be sent to BigQuery to store and used in Looker Studio to create a graphical representation.
