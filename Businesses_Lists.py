import requests
import json

#define search perameters
place = ''
place_type = ''
APIkey = ''

#customer URL to get info from google places api with specified search perameters
url = 'https://maps.googleapis.com/maps/api/place/textsearch/json?query={}+in+{}&key={}'.format(place_type, place, APIkey)

#HTTP get requests on customer URL
page = requests.get(url)

#turn HTTP get request into JSON file
content = page.content
output = json.loads(content)
print(content)

#output all business names and their operational status
for i in range(len(output['results'])):
    print(output['results'][i]['name'],output['results'][i]['business_status'])
