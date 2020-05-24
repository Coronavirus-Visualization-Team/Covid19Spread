import pandas as pd
import requests
import json
import os.path
from os import path

def google_requests(place, place_type, key):
    print("request","place:",place,"Place Type:", place_type)
    #customer URL to get info from google places api with specified search perameters
    url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query={}+in+{}&key={}'.format(place_type, place, key)
    #HTTP get requests on customer URL
    page = requests.get(url)
    #turn HTTP get request into JSON file
    content = page.content
    output = json.loads(content)

    return output

def get_counties(path):
    try:
        df = pd.read_csv(path)
        ls = []
        for i in range(df.index.size):
            ls.append(df.loc[i].values[0])
    except:
        print(path)
        print("Error opening file")
        exit(0)
    
    return [ls, df.index.size]

def main(path, key, frame, places_types, state):
    #try:
    counties_info = get_counties(path)
    for x in range(counties_info[1]):
        for y in range(len(places_types)): 
            info = google_requests(counties_info[0][x], places_types[y], key)
            for i in range(len(info['results'])):
                    nums = info['results'][i]['geometry'].get('location')
                    lat = nums.get('lat')
                    lng = nums. get('lng')   
                    name = info['results'][i]['name']
                    placeid = info['results'][i][place_id']
                    searchTag = places_types[y]
                    series = {'PlaceId': placeid, 'State':state, 'County':counties_info[0][x],'Business Name':name, 'Search Tag': searchTag, 'Latitude': lat, 'Longitude': lng}
                    frame = frame.append(series, ignore_index = True)
                        

    # except:
    #     print("Rate limit error")
    
   
    return frame

if __name__ == "__main__":
    
    places_types = ['Entertainment','Parks','Restaurants','Hospitals','Doctors offices','car repair','retail stores','gyms']

    with open('', 'r') as resources: #use path of json format .txt file to load data
        data = json.load(resources)
        key = data[0].get('key')
        paths = data[0].get('paths')
        states = data[0].get('states')
        outfile = data[0].get('outfile')

        if(not path.exists(outfile)):
            columns = ['ID', 'PlaceId', 'State', 'County', 'Business Name', 'Search Tag', 'Latitude', 'Longitude']
            mainFrame = pd.DataFrame(columns = columns)
        else:
            mainFrame = pd.read_csv(outfile)
    
    for i in range(len(paths)):
        mainFrame  = main(paths[i], key, mainFrame, places_types, states[i])

    mainFrame.to_csv(outfile)
