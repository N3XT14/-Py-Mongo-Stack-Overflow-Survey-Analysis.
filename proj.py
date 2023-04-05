from pymongo import MongoClient, collection
from os import getenv
from dotenv import load_dotenv, find_dotenv

def get_database():

    #Connect python to mongodb atlas using the connection string
    load_dotenv(find_dotenv())
    client = MongoClient(getenv("CONNECTION_STRING"))
    _client_db = client["StackOverflow2022"]
    
    return _client_db

def analyze_tech_stack_preference(data: collection.Collection, data_count: int):

    #To analyze which tech stack is associated with higher salaries and the age group. 
    result = data.aggregate(
        [
            {
                "$match": {
                    "$and": [
                        {
                            "Age": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "ConvertedCompYearly": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "LanguageHaveWorkedWith": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "DatabaseHaveWorkedWith": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "WebframeHaveWorkedWith": { "$exists": True, "$ne": "NA" },
                        }
                    ]
                }
            },
            {
                "$addFields": {
                    "AgeDigits": {
                        "$regexFind": {
                            "input": "$Age",
                            "regex": "\\d+"
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "AgeInt": {
                        "$toInt": "$AgeDigits.match"
                    }
                }
            },
            {
                "$addFields": {
                    "AgeGroup": {
                        "$switch": {
                            "branches": [
                                { "case": { "$lte": [ "$AgeInt", 24 ] }, "then": "Under 25" },
                                { "case": { "$lte": [ "$AgeInt", 34 ] }, "then": "25-35" },
                                { "case": { "$lte": [ "$AgeInt", 44 ] }, "then": "35-45" },
                                { "case": { "$lte": [ "$AgeInt", 54 ] }, "then": "45-55" },
                                { "case": { "$gte": [ "$AgeInt", 55 ] }, "then": "55+" }
                            ]
                        }
                    }
                }
            },
            { 
                "$group": { 
                    "_id": { 
                        "AgeGroup": "$AgeGroup", 
                        "Technology":"$LanguageHaveWorkedWith",
                        "Database": "$DatabaseHaveWorkedWith",
                        "Webframe": "$WebframeHaveWorkedWith"                    
                    },
                    "Count": { "$sum": 1 },
                    "Salary": { "$avg": "$ConvertedCompYearly"}
                } 
            },            
            {"$sort": {"_id.AgeGroup": 1, "Salary": -1}},
            {
                "$group": {
                    "_id": {"AgeGroup": "$_id.AgeGroup"},
                    "TopLanguages": {"$first": {"$split": ["$_id.Technology", ";"]}},
                    "TopDatabases": {"$first": {"$split": ["$_id.Database", ";"]}},\
                    "TopWebframe": {"$first": {"$split": ["$_id.Webframe", ";"]}},
                    "TopSalary": {"$first": "$Salary"},
                    "SecondLanguages": {"$push": {"$split": ["$_id.Technology", ";"]}},
                    "SecondDatabases": {"$push": {"$split": ["$_id.Database", ";"]}},
                    "SecondWebframe": {"$push": {"$split": ["$_id.Webframe", ";"]}},                    
                    "SecondSalary": {"$push": "$Salary"}
                }
            },
            {
                "$addFields": {
                    "SecondLanguages": {"$slice": ["$SecondLanguages", 1, 1]},
                    "SecondDatabases": {"$slice": ["$SecondDatabases", 1, 1]}, 
                    "SecondWebframe": {"$slice": ["$SecondWebframe", 1, 1]}, 
                    "SecondSalary": {"$slice": ["$SecondSalary", 1, 1]}
                }
            },
            {"$project": {
                "AgeGroup": "$_id.AgeGroup",
                "TopLanguages": 1,
                "TopDatabases": 1,
                "TopWebframe": 1,
                "TopSalary": 1,
                "SecondLanguages": {"$arrayElemAt": ["$SecondLanguages", 0]},
                "SecondDatabases": {"$arrayElemAt": ["$SecondDatabases", 0]},
                "SecondWebframe": {"$arrayElemAt": ["$SecondWebframe", 0]},
                "SecondSalary": {"$arrayElemAt": ["$SecondSalary", 0]},
                "_id": 0
            }}            
        ]
    )
    
    return result

def analyze_mental_health_impact(data: collection.Collection, data_count: int):

    result = data.aggregate(
        [        
            {
                "$match": {
                    "$and": [
                        {
                            "MentalHealth": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "Age": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "Gender": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "Ethnicity": { "$exists": True, "$ne": "NA" },
                        }
                    ]
                }
            },
            {
                "$addFields": {
                    "AgeDigits": {
                        "$regexFind": {
                            "input": "$Age",
                            "regex": "\\d+"
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "AgeInt": {
                        "$toInt": "$AgeDigits.match"
                    }
                }
            },
            {
                "$addFields": {
                    "AgeGroup": {
                        "$switch": {
                            "branches": [
                                { "case": { "$lte": [ "$AgeInt", 24 ] }, "then": "Under 25" },
                                { "case": { "$lte": [ "$AgeInt", 34 ] }, "then": "25-35" },
                                { "case": { "$lte": [ "$AgeInt", 44 ] }, "then": "35-45" },
                                { "case": { "$lte": [ "$AgeInt", 54 ] }, "then": "45-55" },
                                { "case": { "$gte": [ "$AgeInt", 55 ] }, "then": "55+" }
                            ]
                        }
                    }
                }
            },
            {
                "$addFields": {
                    "Ethnicity": {
                        "$split": ["$Ethnicity", ";"]
                    }
                }
            },
            {
                "$unwind": "$Ethnicity"
            },
            {
                "$addFields": {
                    "Gender": {
                        "$split": ["$Gender", ";"]
                    }
                }
            },
            {
                "$unwind": "$Gender"
            },
            { 
                "$group": { 
                    "_id": { 
                        "AgeGroup": "$AgeGroup", 
                        "Gender":"$Gender",
                        "Ethnicity": "$Ethnicity",                 
                    },
                    "total_respondents": {"$sum": 1},
                    "total_mental_health_issues": { "$sum": { "$cond": [{ "$eq": ["$MentalHealth", "None of the above"]}, 0, 1]} }
                } 
            },
            {
                "$project": {
                    "_id": 0,
                    "AgeGroup": "$_id.AgeGroup",
                    "Gender": "$_id.Gender",
                    "Ethnicity": "$_id.Ethnicity",
                    "total_respondents": 1,
                    "total_mental_health_issues": 1,
                    "percentage_mental_health_issues": {
                        "$multiply": [{
                            "$divide": ["$total_mental_health_issues", { "$toDouble": "$total_respondents" }]
                        }, 100]
                    }
                }
            },
            {"$sort": {"total_respondents": -1, "total_mental_health_issues": -1}},
            {"$limit": 10}
        ]
    )
    return result

if __name__ == "__main__":

    #Get the database
    stack_db = get_database()

    #Get the collection
    stack_data = stack_db["surveyresult"]

    #Count of total data.
    data_count = stack_data.count_documents({})    
    print("Data Count => ", data_count)

    #Analyze 1: Tech Stack Preference
    analyze_result_1 = analyze_tech_stack_preference(stack_data, data_count)
    print("Task1 Result:")
    for doc in analyze_result_1:
        print(doc)
    print("\n")

    #Analyze 2: Impact on Mental Health
    analyze_result_2 = analyze_mental_health_impact(stack_data, data_count)
    print("Task2 Result:")
    for doc in analyze_result_2:
        print(doc)