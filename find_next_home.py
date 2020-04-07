import csv
import requests
import pandas as pd
from time import sleep
from requests.exceptions import Timeout
import geopy.distance as gd

print('Initializing..')

BASE_URL = r'https://maps.googleapis.com/maps/api/place/'
OUTPUT_TYPE = 'json'
API_KEY = 'AIzaSyA9VV9_anrWaknUBHYEuCHgqKcSWWjsNyk'

def get_response(api,params):
    CALL = f"{BASE_URL}{api}{OUTPUT_TYPE}?key={API_KEY}&{params}"
    print(f"Attempting call to {CALL}")
    try:
        response = requests.get(CALL, timeout=(5,10))
    except Timeout:
        print('The request timed out')
        response = False
    else:
        print('The request did not time out')        
    return response

def get_places(lat,lon,radius,keyword):
    API = r'nearbysearch/'
    PARAMS = f"location={lat},{lon}&radius={radius}&keyword={keyword}"
    response = get_response(API,PARAMS)
    return response

def get_details(place_id):
    API = r'details/'
    PARAMS = f"place_id={place_id}&fields=name,rating,formatted_phone_number,formatted_address,url,website,price_level"
    response = get_response(API,PARAMS)
    return response

def stringClean(string):
    string = string.strip().replace(" ", "-").replace("\\", "-").replace(".", "").replace("/", "-")
    return string

MARTA_STATIONS = r'C:\Users\MLondeen\data\marta_stations.csv'

# get list of dictionaries with some keys from the original request and some keys from a secondary request

total_ct = 0

with open(MARTA_STATIONS) as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    line_count = 0
    print(f'Data loaded..')
    for row in csv_reader:
        result_list = []
        s = row['Station']
        print(f'Retrieving apartments near Station {s}..')
        sleep(1) # make sure the calls are nice and slow
        response = get_places(row['lat'],row['lon'],'900','apartment')

        if response:
            results = response.json()
            print(f'{s} returned {len(results)} results. Attempting to process..')
            for result in results['results']:
                n = result['name']
                print(f'Retrieving details for {n}..')
                
                coords_station = (row['lat'],row['lng'])
                coords_apt =     (result['geometry']['location']['lat'],result['geometry']['location']['lng'])

                print geopy.distance.vincenty(coords_1, coords_2).km

                a = {'place_id':result['place_id'],
                    'name':result['name'],
                    'lat':result['geometry']['location']['lat'],
                    'lng':result['geometry']['location']['lng'],
                    'nearest_station':row['Station'],
                    'distance_to_station':geopy.distance.distance(coords_station,coords_apt).miles}
                
                sleep(1) # make sure the calls are nice and slow
                details = get_details(result['place_id'])
                if details:
                    b = details.json()
                    b = b['result']
                    c = {**a,**b}
                    result_list.append(c)
                    total_ct += 1
                else:
                    result_list.append(a)
        
            print(f'Processing for {s} complete. Persisting to Disk..')
            out_df = pd.DataFrame(result_list)
            out_df.to_csv(stringClean(f'station_{s}_apartments.csv'), index=False)
            print(f'Finished! Moving on to next set..')
        
        else:
            print(f'Request Failed! Moving on to next set..')
        
print(f'Fetched a total of {total_ct} results. Shutting Down..')
