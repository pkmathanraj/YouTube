# YouTube Data harvesting and Warehousing

**About the Project:**

This is a Web Application based on Python - Streamlit which uses Google YouTube API to extract information about any given YouTube channel. It then stores the extracted data in MongoDb and migrates the selected channel data to PostgreSQL Data Warehouse for processing queries to retrieve the results needed.

---

**Table of Contents**

1. Technologies Used
2. Module Requirements and Installation
3. Usage
4. Features
5. Initial Setup
6. Process Flow
7. Contribution
8. License
9. Contact

---

**Technologies Used :**
- Python 
- Streamlit
- Data Management using MongoDB (Atlas)
- PostgreSQL

---

**Module Requirements and Installation :**

Install the following packages before running this project:
```python
pip install google-api-python-client
pip install pymongo
pip install pandas
pip install psycopg2
pip install streamlit
```

---

**Usage**

To use this project, follow these steps:

1. Clone the repository : ```git clone https://github.com/pkmathanraj/YouTube.git```
2. Install the required packages : ```pip install -r requirements.txt```
3. Run the Streamlit app : ```streamlit run YouTubeProject.py```
4. If you are using multiple versions of python then install the packages and run the program by explicitly specifying the version like :
   - For Installing the modules in a specified version : ```py -3.11 -m pip install modulename```
   - For Running the project in a specified version : ```py -3.11 -m streamlit run YouTubeProject.py```

---

**Features :**

- Extract data about a YouTube channel which includes channel info, playlist info, videos info, and comments with replies details
- Store the retrieved data in a MongoDB database
- Migrate the data to a PostgreSQL data warehouse based on user selection
- Extracting results from some predefined set of queries in the PostgreSQL based on user selection
----
**Initial setup after module installation:**
1. Update the YouTube API key using your API Key in the main function
2. Update the MongoDb connection using the connection string with password for your mongodb setup in the main function (Make sure to setup a separate cluster for this project with permission to access from any ip location and admin level access for the user (Not just ReadWrite Access)
3. Modify the database.ini file according to your PostgreSQL setup (Except password all the fields are mostly common)
   * No need to create any database or collection in Mongodb (It'll be automatically created via program)
   * No need to create any database in PostgreSQL (It'll be automatically created via program)
---
**Process Flow :**
1. In main function, the connection to YouTube API is established using the API Key followed by MongoDb Connection
2. After execution of the streamlit web application, user has 4 choices to start with.
3. A user can store the data to MongoDb or PostgreSQL only after some data has been extracted from a youtube channel using the first option "Channel Data Extraction"
4. So, start with "Channel Data Extraction"
5. Then, "Store the data to MongoDb"
6. User can repeat the above two steps to get multiple channel details
7. Then to migrate the data from MongoDb to PostgreSQL, select "Migrating to SQL" option which lists all the available channel both in MongoDb and PostgreSQL.
8. Select the desired channel to migrate and submit to store it in PostgreSQL
9. After migration of required multiple youtube channels, user can view the results of set of predefined queries using "SQL Queries" option.

---

**Future Updates :**
1. Option to extract the youtube channel id directly from the URL
2. Option to limit number of videos to extract
3. Visible Process Loading time through Progress bar
4. Option to remove selected channel data from MongoDb and PostgreSQL
5. Data Analytics of selected channel
6. Data Analysis with Visualization by comparing the multiple channels
7. Multiple SQL Query Results
8. Code Performance Improvement
9. Logging feature

---

**Contribution: **

Contributions to this project are welcome! Please submit a pull request for any issues or suggestions.

---


**License :**

This project is licensed under the MIT License. Please review the LICENSE file for more details.

---

**Contact :**

📧 Email: pkmathanraj@gmail.com 

🌐 LinkedIn: [pkmathanraj](https://www.linkedin.com/in/pkmathanraj/)
