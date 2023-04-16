from pymongo import MongoClient, collection
from os import getenv
from dotenv import load_dotenv, find_dotenv
import matplotlib.pyplot as plt
import numpy as np
import textwrap
import json
import pandas as pd
from prettytable import PrettyTable

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
                            "LanguageHaveWorkedWith": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "Country": {"$in": ["United States of America", "India", "Canada", "Australia", "United Kingdom of Great Britain and Northern Ireland"]},
                        },
                        {
                            "WebframeHaveWorkedWith": { "$exists": True, "$ne": "NA" },
                        },
                        {
                            "CompTotal": { "$exists": True, "$ne": "NA" }
                        },
                        {
                            "CompFreq": "Yearly"
                        }
                    ]
                }
            },
            {
                "$addFields": {
                    "LanguageList": {"$split": ["$LanguageHaveWorkedWith", ";"]},
                    "WebframeList": {"$split": ["$WebframeHaveWorkedWith", ";"]}
                }
            },
            {
                "$unwind": "$LanguageList"
            },
            {
                "$unwind": "$WebframeList"
            },
            {
                "$group": {
                    "_id": {
                        "Country": "$Country",
                        "TechnologyStack": {"$concat": ["$LanguageList", ";", "$WebframeList"]},
                        "CompFreq": "$CompFreq"
                    },
                    "Count": {"$sum": 1},
                    "CompTotal": { "$avg": "$CompTotal" }
                }
            },
            {
                "$group": {
                    "_id": {
                        "Country": "$_id.Country",
                    },
                    "TechnologyStacks": {
                        "$push": {
                            "TechnologyStack": "$_id.TechnologyStack",
                            "Count": "$Count",
                            "CompTotal": "$CompTotal",
                            "CompFreq": "$_id.CompFreq"
                        }                        
                    },
                    "TotalDevelopers": {"$sum": "$Count"}
                }
            },
            {
                "$addFields": {
                    "DominantStack": {
                        "$reduce": {
                            "input": "$TechnologyStacks",
                            "initialValue": {"TechnologyStack": "", "Count": 0},
                            "in": {
                                "$cond": {
                                    "if": {"$gt": ["$$this.Count", "$$value.Count"]},
                                    "then": "$$this",
                                    "else": "$$value"
                                }
                            }
                        }
                    }
                }
            },
            {
                "$sort": {"TotalDevelopers": -1}
            },
            {
                "$project": {
                    "Country": "$_id.Country",
                    "DominantStack": 1,
                    "LeastDominantStack": {"$arrayElemAt": ["$TechnologyStacks", -1]},
                    "TotalDevelopers": 1,
                    "_id": 0
                }
            },
            {
                "$limit": 5
            }
        ]
    )
    
    return result

def analyze_mental_health_impact(data: collection.Collection, data_count: int):

    result = data.aggregate(
        [        
            {
                "$match": {
                    "$and": [
                        { "MentalHealth": { "$exists": True, "$ne": "NA" } },
                        { "Gender": 
                            {"$regex": "^(?!.*(?:Or, in your own words:|Prefer not to say)).*$", "$ne": "NA"}
                        },
                        { "Ethnicity": 
                            {"$regex": "^(?!.*(?:Or, in your own words:|Prefer not to say)).*$", "$ne": "NA"}
                        }
                    ]
                }
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
                    "CodingActivities": {
                        "$split": ["$CodingActivities", ";"]
                    },
                    "PurchaseInfluence": {
                        "$split": ["$PurchaseInfluence", ";"]
                    }
                }
            },
            {
                "$addFields": {
                    "coding_activities_count": {
                        "$size": "$CodingActivities"
                    }
                }
            },
            {
                "$group": {
                    "_id": {
                        "Gender": "$Gender",
                        "Ethnicity": "$Ethnicity",
                    },                        
                    "total_respondents": { "$sum": 1 },
                    "coding_activities_count": { "$max": "$coding_activities_count" },
                    "total_mental_health_issues": {
                        "$sum": {
                            "$cond": [
                                { 
                                    "$and": [
                                { "$ne": [ "$MentalHealth", "None of the above" ] },
                                { "$gt": [ "$coding_activities_count", 2 ] },
                                { "$in": [ "I have a great deal of influence", "$PurchaseInfluence" ] }
                                ]
                                },
                                1,
                                0
                            ]
                        }
                    },
                    "likely_mental_health_issues": {
                        "$sum": {
                        "$cond": [
                            {
                            "$and": [
                                { "$eq": [ "$MentalHealth", "None of the above" ] },
                                { "$gt": [ "$coding_activities_count", 2 ] },
                                { "$in": [ "I have a great deal of influence", "$PurchaseInfluence" ] }
                            ]
                            },
                            1,
                            0
                        ]
                        }
                    }
                }
            },
            {
                "$addFields": {
                "percentage_mental_health_issues": {
                    "$multiply": [
                    { "$divide": [ "$total_mental_health_issues", "$total_respondents" ] },
                    100
                    ]
                },
                "percentage_likely_mental_health_issues": {
                    "$multiply": [
                    { "$divide": [ "$likely_mental_health_issues", "$total_respondents" ] },
                    100
                    ]
                }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "Gender": "$_id.Gender",
                    "Ethnicity": "$_id.Ethnicity",
                    "total_respondents": 1,                
                    "percentage_mental_health_issues": 1,
                    "percentage_likely_mental_health_issues": 1,
                    "coding_activities_count": 1
                }
            },
            {
                "$sort": { "likely_mental_health_issues": -1 }
            },
            {
                "$limit": 5
            }
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

def self_taught_dev_landed_job(data: collection.Collection, data_count: int):

    result = data.aggregate(
        [
            {
                "$match": {
                    "$and": [
                        {
                            "MainBranch": { 
                                "$exists": True, 
                                "$in": ["I am a developer by profession", "I used to be a developer by profession, but no longer am"] }
                        },
                        {
                            "LearnCode": {
                                "$regex": "^(?!.*(?:Coding Bootcamp|School|Online Courses or Certification)).*$", "$ne": "NA"
                            },
                        },
                        {
                            "Employment": "Employed, full-time"
                        }
                    ]
                }
            }        
        ]
    )
    return result

def plot_analyze_result_1(result: collection.Collection):
    data = []
    for i in result:
        data.append(i)

    table = PrettyTable()
    table.field_names = ["Country", "Total Developers", "Dominant Technology Stack", "Dominant Count", "Dominant CompTotal", "Least Dominant Technology Stack", "Least Dominant Count", "Least Dominant CompTotal"]

    for item in data:
        table.add_row([item["Country"],
                    item["TotalDevelopers"],
                    item["DominantStack"]["TechnologyStack"],
                    item["DominantStack"]["Count"],
                    item["DominantStack"]["CompTotal"],
                    item["LeastDominantStack"]["TechnologyStack"],
                    item["LeastDominantStack"]["Count"],
                    item["LeastDominantStack"]["CompTotal"]])    
    print(table)
    return table

def plot_analyze_result_2(data: collection.Collection, data_count: int):
    
    # Create a list of dictionaries containing the data to plot
    sdata = []
    max_actual, max_likely = float("-inf"), float("-inf")
    min_actual, min_likely = float("inf"), float("inf")    
    for doc in analyze_result_2:        
        max_actual = max(doc['percentage_likely_mental_health_issues'], max_actual)
        max_likely = max(doc['percentage_likely_mental_health_issues'], max_likely)
        min_actual = min(doc['percentage_likely_mental_health_issues'], min_actual)
        min_likely = min(doc['percentage_likely_mental_health_issues'], min_likely)
        sdata.append(doc)
    
    # Create a list of genders and their corresponding colors
    genders = ['Man', 'Woman', 'Non-binary']

    # Create two subplots side-by-side
    fig, ax = plt.subplots(1, 2, figsize=(10, 5))

    for i in range(2):
        ax[i].bar([d['Ethnicity'] + " \n" + d['Gender'][:5] for d in sdata], [d['percentage_mental_health_issues'] if i == 0 else d['percentage_likely_mental_health_issues'] for d in sdata])
        ax[i].set_title('Mental Health Issues' if i == 0 else 'Likely Mental Health Issues')
        ax[i].set_xlabel('Ethnicity')
        ax[i].set_ylabel('Percentage')
        ax[i].set_ylim([min_actual, max_actual+10] if i == 0 else [min_likely, max_likely+10])
        ax[i].tick_params(axis='x')
        ax[i].legend(genders, loc='upper left')
        
    # Add text on each bar chart of coding_activities_count
    for i, v in enumerate(sdata):
        ax[0].annotate(str(v['total_respondents']), xy=(i, v['percentage_mental_health_issues']), 
        ha='center', va='bottom')
        ax[1].annotate(str(v['coding_activities_count']), xy=(i, v['percentage_likely_mental_health_issues']), ha='center', va='bottom')

    # Set the overall title of the plot
    fig.suptitle('Mental Health Issues by Gender and Ethnicity')

    # Display the plot
    plt.show()

    return

def plot_analyze_result_3(data: collection.Collection, data_count: int):
    
    #Data Preparation
    age_groups, compensation_remote, compensation_hybrid = ['Under 25', '25-35', '35-45', '45-55', '55+'], [], []
    for doc in analyze_result_3:
        # data for the chart        
        if doc['RemoteWork'] == "Fully remote":
            compensation_remote.append(doc['AvgCompensation'])
        else:
            compensation_hybrid.append(doc['AvgCompensation'])    

    # set up the figure and axes
    fig, ax = plt.subplots(figsize=(10, 6))

    # plot the data
    bar_width = 0.35
    opacity = 0.8
    index = np.arange(len(age_groups))

    ax.bar(index, compensation_remote, bar_width, alpha=opacity, color='b', label='Fully remote')

    ax.bar(index + bar_width, compensation_hybrid, bar_width, alpha=opacity, color='g', label='Hybrid')

    # add labels and title
    ax.set_xlabel('Age groups')
    ax.set_ylabel('Average Compensation')
    ax.set_title('Average Compensation by Age group and Remote Work preference')
    ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels(age_groups)
    ax.legend()

    # display the chart
    plt.show()
    
    return

def plot_analyze_result_4(data: collection.Collection, count: int):

    self_taught_count = len(list(data))
    self_taught_perc = round((self_taught_count/count)*100, 2)
    approach_followed = ["Self Taught", "Traditional Approach"]
    approach_followed_perc = [self_taught_perc, 100 - self_taught_perc]

    fig = plt.gcf()
    fig.set_facecolor("white")
    fig.set_size_inches(7,5)

    plt.pie(approach_followed_perc, labels=approach_followed, autopct='%1.1f%%', explode=[0.1, 0], startangle=90)
    plt.title("Landed Full Time Job as Developer")
    plt.axis('equal')
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
    print("Task1 Result")
    plot_analyze_result_1(analyze_result_1)

    #Analyze 2: Impact on Mental Health
    print("Task2")
    analyze_result_2 = analyze_mental_health_impact(stack_data, data_count)    
    plot_analyze_result_2(analyze_result_2, data_count)
    print("\n")

    # #Analyze 3: Impact of Remote Work on Age Group
    print("Task3")
    analyze_result_3 = analyze_remote_work_impact(stack_data, data_count)
    plot_analyze_result_3(analyze_result_3, data_count)
    print("\n")

    #Analyze 4: Percentage of Self Taught vs Traditional Learning that landed full time job as developer
    print("Task4")
    analyze_result_4 = self_taught_dev_landed_job(stack_data, data_count)    
    plot_analyze_result_4(analyze_result_4, data_count)
    print("\n")
