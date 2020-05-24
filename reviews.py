import requests
import json
import pandas as pd
import pprint as pp
import csv
from private import apiKey

'''
Authors: Felix.Y, Rayne.S
'''
headers = {'Authorization': 'bearer %s' % apiKey}

file = pd.read_csv("Businesses_Output.csv", encoding="utf-8")

# Slicing the dataframe to start at a different index
file = file[:25]

#sets up fields for reviews csv
fields = ['ID', 'Review']
filename = "BusinessReviews.csv"

retDF = pd.DataFrame(columns=['id', 'reviews'])

# Note: It can scrape at max 2500 businesses at a time (rate limit 5000/day)
for index, row in file.iterrows():
    search = requests.get('https://api.yelp.com/v3/businesses/search?term=' + row['Business Name'] + '&latitude=' + str(row['Latitude']) + '&longitude=' + str(row['Longitude']), headers=headers).json()
    for business in search['businesses']:
        if row['Business Name'] == business['name'] and abs(row['Latitude'] - business['coordinates']['latitude']) < 0.001 and abs(row['Longitude'] == business['coordinates']['longitude']) < 0.001:
            businessID = business['id']
            reviews = reviews = requests.get('https://api.yelp.com/v3/businesses/' + businessID + '/reviews', headers=headers).json()
            specific_rev = reviews['reviews']
            for review in reviews['reviews']:
                mydict = [{'ID': index, 'Review': review["text"]}]
                with open(filename, 'a') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fields)
                    writer.writerows(mydict)
