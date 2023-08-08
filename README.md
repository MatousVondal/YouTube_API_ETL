# YouTube_API_ETL
Within my project, I've  orchestrated a YouTube API ETL pipeline using Airflow. This effective process extracts data, transforms it into structured frames, and ensures accuracy via data validation. The subsequent loading into Google BigQuery sets the stage for comprehensive analytics. For analytic process will be use Power BI.

# Why YouTube API?
I chose the YouTube API as the foundation for my project due to its unparalleled ability to grant me access to a vast and dynamic repository of real-time data. With its extensive coverage of video content, channel metrics, comments, and more, the YouTube API empowers my analysis with a diverse range of insights.
![Steps](infographic.png)

# Flow chart 
![Steps](flow_chart.png)

# Airflow 

This Airflow Directed Acyclic Graph (DAG) orchestrates a robust ETL (Extract, Transform, Load) pipeline leveraging the YouTube API. The pipeline involves extracting data from the YouTube API, transforming and validating it, and finally exporting it to Google BigQuery for advanced analytics.

Tasks:

Extract and Transform Task: This task extracts data from the YouTube API using a provided API key and conducts initial data transformations. The youtube_statistics is utilized for efficient data analysis.
Data Validation Task: The extracted data is subjected to rigorous validation in this task. The validation process ensures data integrity, accuracy, and adherence to predefined data types and structure.
Export to BigQuery Task: Validated data is loaded into Google BigQuery for further processing and analysis. The task uses the google.cloud library and requires appropriate credentials for access.
DAG Configuration:

Default Arguments: The DAG is configured with default arguments that manage task retries, email notifications, and scheduling settings.
Scheduling: The DAG is scheduled to run at specific intervals (6:00 AM, 12:00 PM, and 6:00 PM) using a cron expression (0 6,12,18 * * *).
Error Handling: In case of task failure, email notifications are triggered to keep stakeholders informed about the status of the pipeline.
Dependencies:

The tasks within the DAG are sequenced using the "bitwise left shift" (>>) operator, ensuring a logical order of execution. The extract_and_transform_task is executed first, followed by the data_validation_task, and finally, the export_to_bigquery_task.

Note:

Ensure that the necessary API keys, credentials, and libraries (youtube_statistics, google.cloud) are appropriately configured and accessible.
Be mindful of any changes to API terms of use, data schemas, or access permissions that may impact the pipeline.
This DAG provides a comprehensive solution for efficiently processing YouTube data through a structured ETL workflow, allowing for meaningful insights and informed decision-making.

Feel free to adapt and modify this description to best match the specifics of your project and DAG setup.
