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
    print("request","name: ", name, "Latitude: ", lat, "Longitude: ", long)
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
    print("request", "place_id ", place_id)
    url = "https://maps.googleapis.com/maps/api/place/details/json?place_id={}&key={}".format(place_id, key)
    page = requests.get(url)
    content = page.content
    output = json.loads(content)
    return output

def search(start, end, df, dicToAdd):
    listFormattedID = [] #correct ID corresponding to review
    listNames = []
    listLat = []
    listLong = []
    listReview = []
    listID = []
    listJSON = []
    
    for i in range(start, end): #adds contents of split up csv
        listID.append(df["ID"][i])
        listNames.append(df["Business Name"][i])
        listLat.append(df["Latitude"][i])
        listLong.append(df["Longitude"][i])
        
    for i in range(len(listNames)):
       # print(i)
        info = (search_location(listNames[i], "AIzaSyD8Iaqb9uVapwwo8C2-nkHKwXDuys6Ql7U", listLat[i], listLong[i])) 
        j = 0
        redoCounter = 0
        while(j < len(info["results"])):
            try:
                moreInfo = get_business_reviews(info["results"][j]["place_id"], "AIzaSyD8Iaqb9uVapwwo8C2-nkHKwXDuys6Ql7U")
                listJSON.append(moreInfo) #puts all the review json files into a list
                j += 1
            except (TimeoutError, MemoryError, OSError, SystemError, RuntimeError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                if(redoCounter < 10):
                    redoCounter += 1
                else:
                    file = open("errorlog.txt","w")
                    file.write("Failed at index: ", i)
                    file.close()
                    break
            
    for i in range(len(listJSON)):
        info = listJSON[i] #iterates through every compiled review json file
        ID = listID[i] #index of the json file of reviews corresponds with the ID of the place
        if("reviews" in info["result"].keys()): #if no reviews have been made, then don't execute anything
            for j in range(len(info["result"]["reviews"])):
                review = info["result"]["reviews"][j]["text"]
                if(not review == ""): #omits all the empty text
                    listReview.append(review)
                    listFormattedID.append(ID)
                    
    dicToAdd = {"Business ID": listFormattedID, "Reviews": listReview}

try:
    df = pd.read_csv(r"C:\Users\novac\Documents\cvt\Businesses_Output.csv").head(10) #loads csv of compiled businesses
except IOError:
    print("Error Opening File")
    exit(0)

numPartitions = int(input("How many partitions for the csv?: "))
numCols = int(len(df.index)/numPartitions) 
now = datetime.now() 
current_time = now.strftime("%H:%M:%S")
print(current_time)
listCSV = []
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
for i in range(len(listStarting)):
    threads.append(threading.Thread(target = search, args = (listStarting[i], listEnding[i], df, listDics[i])))
for thread in threads:
    thread.start()
for thread in threads:
    thread.join() 
now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("Done with writing at ", current_time)
listAllID = []
listAllReviews = []
for dic in listDics: #appends all contents in smaller csv files into final big one
    print(dic)
   # for i in range(len(dic["Business ID"])):
     #   listAllID.append(dic["Business ID"][i])
      #  listAllReviews.append(dic["Reviews"][i])
finalFormat = {"Business ID": listAllID, "Reviews": listAllReviews}
finalCSV = pd.DataFrame(finalFormat)
finalCSV.to_csv("collective_business_reviews.csv", index = False)
