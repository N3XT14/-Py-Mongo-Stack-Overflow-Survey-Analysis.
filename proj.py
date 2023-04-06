from pymongo import MongoClient, collection
from os import getenv
from dotenv import load_dotenv, find_dotenv
import matplotlib.pyplot as plt
import numpy as np
import textwrap
import json
import pandas as pd

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

def analyze_remote_work_impact(data: collection.Collection, data_count: int):

    result = data.aggregate(
        [        
            {
                "$match": {
                    "$and": [
                        {
                            "MainBranch": { "$exists": True, "$eq": "I am a developer by profession" }
                        },
                        {
                            "Employment": "Employed, full-time",
                        },
                        {
                            "$or": [
                                { "RemoteWork": { "$exists": True, "$eq": "Fully remote" } },
                                { "RemoteWork": { "$exists": True, "$eq": "Hybrid (some remote, some in-person)" } }
                            ]
                        },
                        {
                            "YearsCodePro": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "ConvertedCompYearly": { "$exists": True, "$ne": "NA" },
                        }
                    ],
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
                        "Age": "$AgeGroup",
                        "RemoteWork": "$RemoteWork"
                    },
                    "AvgCompensation": { "$avg": "$ConvertedCompYearly"},
                    "AvgYearsExp": { "$avg": "$YearsCodePro"},
                    "Count": { "$sum": 1}
                }
            },
            {
                "$project":{
                    "_id": 0,
                    "Age": "$_id.Age",
                    "RemoteWork": "$_id.RemoteWork",
                    "AvgCompensation": 1,
                    "AvgYearsExp": 1,
                    "Count": 1
                }
            },
            {"$sort": {"Age": 1, "RemoteWork": 1}},
        ]
    )
    return result

def plot_analyze_result_2(labels: list, values: list):
    
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(labels, values)

    # Axis Configuration
    ax.set_title('Effect of work on mental health')
    ax.set_xlabel('Percentage of respondents with mental health issues')
    ax.set_ylabel('Age Group, Gender, and Ethnicity')

    # Rotate x-axis labels
    plt.xticks(rotation=45)

    # Show plot
    plt.show()

    return

def plot_analyze_result_3(D1: list, D2: list, D3: list,):
    
    # set up the figure and axes
    fig, ax = plt.subplots(figsize=(10, 6))

    # plot the data
    bar_width = 0.35
    opacity = 0.8
    index = np.arange(len(age_groups))

    ax.bar(index, D2, bar_width, alpha=opacity, color='b', label='Fully remote')

    ax.bar(index + bar_width, D3, bar_width, alpha=opacity, color='g', label='Hybrid')

    # add labels and title
    ax.set_xlabel('Age groups')
    ax.set_ylabel('Average Compensation')
    ax.set_title('Average Compensation by Age group and Remote Work preference')
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels(D1)
    ax.legend()

    # display the chart
    plt.show()
    
    return

if __name__ == "__main__":

    #Get the database
    stack_db = get_database()

    #Get the collection
    stack_data = stack_db["surveyresult"]

    #Count of total data.
    data_count = stack_data.count_documents({})    
    print("Data Count => ", data_count, "\n")

    #Analyze 1: Tech Stack Preference
    analyze_result_1 = analyze_tech_stack_preference(stack_data, data_count)
    print("Task1 Result:")
    data = []
    for doc in analyze_result_1:
        data.append(doc)

    def pretty_json_for_savages(j, indentor='  '):
        ind_lvl = 0
        temp = ''
        for i, c in enumerate(j):
            if c in '{[':
                print(indentor*ind_lvl + temp.strip() + c)
                ind_lvl += 1
                temp = ''
            elif c in '}]':
                print(indentor*ind_lvl + temp.strip() + '\n' + indentor*(ind_lvl-1) + c, end='')
                ind_lvl -= 1
                temp = ''
            elif c in ',':
                print(indentor*(0 if j[i-1] in '{}[]' else ind_lvl) + temp.strip() + c)
                temp = ''
            else:
                temp += c
        print('')

    pretty_json_for_savages(json.dumps(data))
    print("\n")    

    #Analyze 2: Impact on Mental Health
    analyze_result_2 = analyze_mental_health_impact(stack_data, data_count)
    print("Task2 Result:")

    labels, values = [],[]
    for doc in analyze_result_2:
        labels.append(doc['AgeGroup'] + ' ' + doc['Gender'] + ' ' + doc['Ethnicity'])
        values.append(doc['percentage_mental_health_issues'])

    plot_analyze_result_2(labels, values)
    print("\n")

    #Analyze 3: Impact of Remote Work on Age Group
    analyze_result_3 = analyze_remote_work_impact(stack_data, data_count)
    print("Task3 Result:")

    age_groups, compensation_remote, compensation_hybrid = ['Under 25', '25-35', '35-45', '45-55', '55+'], [], []
    for doc in analyze_result_3:
        # data for the chart        
        if doc['RemoteWork'] == "Fully remote":
            compensation_remote.append(doc['AvgCompensation'])
        else:
            compensation_hybrid.append(doc['AvgCompensation'])    

    plot_analyze_result_3(age_groups, compensation_remote, compensation_hybrid)
    