import requests
import json
import pandas as pd
import pprint as pp

apiKey = ''
headers = {'Authorization': 'bearer %s' % apiKey}

file = pd.read_csv("Businesses_Output.csv", encoding="utf-8")

# Slicing the dataframe to start at a different index
#file = file[index:]

for index, row in file.iterrows():
    search = requests.get('https://api.yelp.com/v3/businesses/search?term=' + row['Business Name'] + '&latitude=' + str(row['Latitude']) + '&longitude=' + str(row['Longitude']), headers=headers).json()
    for business in search['businesses']:
        if row['Business Name'] == business['name'] and abs(row['Latitude'] - business['coordinates']['latitude']) < 0.001 and abs(row['Longitude'] == business['coordinates']['longitude']) < 0.001:
            businessID = business['id']
            reviews = reviews = requests.get('https://api.yelp.com/v3/businesses/' + businessID + '/reviews', headers=headers).json()
            print(index)
            print(reviews)

