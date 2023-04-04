from pymongo import MongoClient
from os import getenv
from dotenv import load_dotenv, find_dotenv

def get_database():

    #Connect python to mongodb atlas using the connection string
    load_dotenv(find_dotenv())
    client = MongoClient(getenv("CONNECTION_STRING"))
    _client_db = client["StackOverflow2022"]
    
    return _client_db

if __name__ == "__main__":

    #Get the database
    stack_db = get_database()

    #Get the collection
    stack_data = stack_db["surveyresult"]

    #Count of total data.
    data_count = stack_data.count_documents({})    
    print("Data Count => ", data_count)