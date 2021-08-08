# Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.
# State and justify your database schema design and ETL pipeline.
The Sparkify is going to use two datasets log_data and song_data to create a database to do some analysis.
Both of the datasets are json files.So the steps should be like following:
1.create a database
2.create one fact table and four dimension tables. It uses a star schema.
3.Doing the ETL steps. 
    Load the files and make the data in those files into dataframes. 
    Select the wanted data to create new dataframes
    Load those new dataframes into database by sql insert query.
4.By test.ipynb to query the database and verify.