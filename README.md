# Youtube Data Harvesting and Warehousing

## Take-away Skills :
1. Python
2. MongoDB
3. Postgre SQL
4. Streamlit
5. Google-Client-Library

## Problem Statement :
Create a user interactive Streamlit application that allows the user to enter a Youtube Channel's ID, further allowed to access and analyze the data of the channel.
To achieve this, we had to import certain packages,namely : googleapiclient,streamlit,pandas,psycopg2 and pymongo.

## TAB 1: ADD
1. We have retrieved data from the Youtube API upon the user's selection of ID.
2. The retrived data holds information of the Channel's information, video information and comments information.
3. The retrieved data is then transfered to MongoDB upon clicking the button.
4. Another input is given wherein the user is asked to enter the Channel ID that is wanted to migrate to SQL.
5. Upon its migration to SQL, tables namely : channels,videos, and comments are created.

## Tab 2 : VIEW
In this tab, we will be able to view the created tables alongside the data inserted.

## Tab 3 : QUERY
1. A select box is given with 10 questions.
2. Upon selection of each question, the data is filtered accordingly and displayed to the user.
3. The filteration is done by performing SQL queries.

## Conclusion : 
The project of harvesting and warehousing YouTube data has provided valuable insights into user behavior, content trends, and platform dynamics. Through meticulous data collection and systematic storage, we have been able to analyze vast amounts of information, uncovering patterns and correlations that inform content creators, marketers, and platform developers alike. We have honed our skills in various aspects of python development, usage of MongoDB and building SQL queries.


