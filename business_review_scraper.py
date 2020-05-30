import pandas as pd
from datetime import datetime
import requests
import json
import threading
import os 


#this method searches for all the businesses of the passed in name within a certain radius of the coordinates
#@param name the name of the location
#@param key the api key
#@param lat the latitude cord
#@param long the longitude cord
#@return output the dictionary containing all the json file contents
def search_location(name, key, lat, long):
    #print("request","name: ", name, "Latitude: ", lat, "Longitude: ", long)
    url = 'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={}&radius={}&keyword={}&key={}'.format(str(lat) + "," + str(long), 5, name, key)
    page = requests.get(url) #gets webpage using api request converted into json
    content = page.content
    output = json.loads(content)
    return output 

#this method gets all the reviews corresponding to a passed in place_id
#@param place_id the google place_id of the location
#@param key the api key
#@return output the dictionary containing all the json file contents
def get_business_reviews(place_id, key):
    #print("request", "place_id ", place_id)
    url = "https://maps.googleapis.com/maps/api/place/details/json?place_id={}&key={}".format(place_id, key)
    page = requests.get(url)
    content = page.content
    output = json.loads(content)
    return output

#this method compiles all relevant data and formats
#@param start the starting index of search
#@param end the ending index of search
#@param df the dataframe to read data from
#@param index the index of the thread in correlation with the other threads
#@return 
def search(start, end, thread_id):
    listFormattedID = [] 
    dic = {}
    
    for i in range(start, end): #adds contents of split up csv
        ID = df["ID"][i]
        name = df["Business Name"][i]
        lat = df["Latitude"][i]
        long = df["Longitude"][i]
        redoCounter = 0
        try:
            search = search_location(name, key, lat, long)
        except (TimeoutError, MemoryError, OSError, SystemError, RuntimeError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if(redoCounter < 10):
                redoCounter += 1
            else:
                print("ERROR: Max Retry Limit Reached")
                file = open("errorlog.txt","w")
                file.write("Thread-", thread_id)
                file.close()
                break 
        j = 0
        while(j < len(search["results"])):
            try:
                info = get_business_reviews(search["results"][j]["place_id"], key)
                if("reviews" in info["result"].keys()): #if no reviews have been made, then don't execute anything
                    reviewList = []
                    for k in range(len(info["result"]["reviews"])):
                        review = info["result"]["reviews"][k]["text"]
                        if(not review == ""): #omits all the empty text
                            reviewList.append(review)
                    reviewDic = {ID: reviewList} #stores Business ID correlated with list of reviews
                    dic.update(reviewDic)
                j += 1
                
            except (TimeoutError, MemoryError, OSError, SystemError, RuntimeError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if(redoCounter < 10):
                    redoCounter += 1
                else:
                    print("ERROR: Max Retry Limit Reached")
                    file = open("errorlog.txt","w")
                    file.write("Thread-", thread_id)
                    file.close()
                    break
                    
    listDics[thread_id] = dic #gets thread output in local master dictionary 

try:
    df = pd.read_csv(path) #loads csv of compiled businesses
except IOError:
    print("Error Opening File")
    exit(0)

numPartitions = int(input("How many partitions for the csv?: "))
numCols = int(len(df.index)/numPartitions) 
#now = datetime.now()for testing only
#current_time = now.strftime("%H:%M:%S")
#print("business_review_scraper.py started at: ", current_time)
listStarting = []
listEnding = []
listDics = []
for i in range(numPartitions):
    listStarting.append(int(i * numCols))
    dicToAppend = {}
    if(not i == (numPartitions - 1)):
        listEnding.append((i + 1) * numCols) #splits up csv
    else:
        listEnding.append(len(df.index)) #final split takes till eof
    listDics.append(dicToAppend)
    
threads = []
for i in range(numPartitions):
    threads.append(threading.Thread(target = search, args = (listStarting[i], listEnding[i], i)))
    threads[i].start() #starts each thread
    
for thread in threads:
    thread.join()
    
#now = datetime.now() for testing only
#current_time = now.strftime("%H:%M:%S")
#print("Finished writing to collective_business_reviews.csv at ", current_time)
listAllID = []
listAllReviews = []
for dic in listDics: #appends all contents in dictionaries into 2 final lists for dataframe formatting
    for key in dic:
        for review in dic[key]:
            listAllID.append(key)
            listAllReviews.append(review)
finalFormat = {"Business ID": listAllID, "Reviews": listAllReviews}
finalCSV = pd.DataFrame(finalFormat)
finalCSV.to_csv("collective_business_reviews.csv", index = False)
