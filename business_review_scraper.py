import pandas as pd
import requests
import json

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

try:
    df = pd.read_csv(path) #loads csv of compiled businesses
except IOError:
    print("Error Opening File")
    exit(0)
    
listFormattedID = [] #correct ID corresponding to review
listNames = []
listLat = []
listLong = []
listReview = []
listID = []
listJSON = []
for i in range(len(df.index)):
    listID.append(df["ID"][i])
    listNames.append(df["Business Name"][i])
    listLat.append(df["Latitude"][i])
    listLong.append(df["Longitude"][i])
for i in range(len(listNames)):
    info = (search_location(listNames[i], key, listLat[i], listLong[i])) 
    for j in range(len(info["results"])):
        moreInfo = get_business_reviews(info["results"][j]["place_id"], key)
        listJSON.append(moreInfo) #puts all the review json files into a list
for i in range(len(listJSON)):
    info = listJSON[i] #iterates through every compiled review json file
    ID = listID[i] #index of the json file of reviews corresponds with the ID of the place
    for j in range(len(info["result"]["reviews"])):
        review = info["result"]["reviews"][j]["text"]
        if(not review == ""): #omits all the empty text
            listReview.append(review)
            listFormattedID.append(ID)
finalFormat = {"Business ID": listFormattedID, "Reviews": listReview}
df2 = pd.DataFrame(finalFormat)
df2.to_csv("collective_business_reviews.csv", index = False)
