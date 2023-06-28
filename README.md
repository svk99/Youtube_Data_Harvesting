YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit

Problem Statement-
The problem statement is to create a Streamlit application that allows users to access
and analyze data from multiple YouTube channels

Technologies used-
Python
Streamlit
MongoDB
MySQL
Google API Client Library

Approach

1.Set up a Streamlit application using the python library streamlit, which allows users to enter a YouTube channel ID, view channel details, and select channels to migrate.
2.Connect to the YouTube API V3, which allows  to retrieve channel and video data by using the Google API client library for Python to make request to the API. 
3.Store the retrieved data in a MongoDB database, as MongoDB can handle unstructured and semi-structured data. This is done by calling a  method to retrieve the data from api and storing it in the database.
4.Transfer the collected data from multiple channels to a SQL data warehouse, use database like MySQL for this purpose.
5.Use SQL queries to join tables in the SQL data warehouse and retrieve data for specific channel based on user input. Use mysql.connector for interacting with SQL Database.
6.The retrieved data can be displayed in the Streamlit application using Streamlit's data visualization features to create charts and graphs to help users to analyze the data.
