import csv
import pandas as pd
from time import sleep
from requests import get
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
        response = get(CALL, timeout=(5,10))
    except Timeout:
        print('The request timed out')
        response = False
    else:
        print('The request did not time out')        
        return response

def get_places(lat,lng,radius,keyword):
    API = r'nearbysearch/'
    PARAMS = f"location={lat},{lng}&radius={radius}&keyword={keyword}"
    return get_response(API,PARAMS)

def get_next_pg(next_page_token):
    API = r'nearbysearch/'
    PARAMS = f"next_page_token={next_page_token}"
    return get_response(API,PARAMS)

def get_details(place_id):
    API = r'details/'
    PARAMS = f"place_id={place_id}&fields=name,rating,formatted_phone_number,formatted_address,url,website,price_level"
    return get_response(API,PARAMS)

def stringClean(string):
    return string.strip().replace(" ", "-").replace("\\", "-").replace(".", "").replace("/", "-")
     
def process_response(response):
    print(f'This station has {len(response.json())} nearby apartments. Attempting to process..')
    result_list = []
    for result in response.json()['results']:                
        coords_station = (row['lat'],row['lng'])
        coords_apt     = (result['geometry']['location']['lat'],result['geometry']['location']['lng'])                
        apt = {'place_id':result['place_id'],
               'name':result['name'],
               'lat':result['geometry']['location']['lat'],
               'lng':result['geometry']['location']['lng'],
               'nearest_station':row['Station'],
               'distance_to_station':gd.distance(coords_station,coords_apt).miles}                
        sleep(.1) # make sure the calls are nice and slow
        details = get_details(result['place_id'].json()['result'])
        if details:
            result_list.append({**apt,**details})
            total_ct += 1
        else:
            result_list.append(apt)
    return result_list
        

def build_request(input):

def persist_data(data,station):
    print('Processing for station complete. Persisting to Disk..')
    pd.DataFrame(data).to_csv(stringClean(f'station_{station}_apartments.csv'), index=False)

MARTA_STATIONS = r'.\data\marta_stations_test.csv'

total_ct = 0

# driver
with open(MARTA_STATIONS) as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    line_count = 0
    print(f'Data loaded..')
    for row in csv_reader:
        station = row['Station']
        print(f'Retrieving apartments near Station {station}..')
        sleep(.1) # make sure the calls are nice and slow
        response = get_places(row['lat'],row['lng'],'900','apartment')
        apartment_list = []
        if response:
            apartment_list = apartment_list**, process_response(response)**)
            if 'next_page_token' in response.json():
                response = get_places(row['lat'],row['lng'],'900','apartment')
                process_response(response)
            else:
                print(f'Finished current station Moving on to next station..')
            persist_data(apartment_list,station)
        else:
            print(f'Request Failed! Moving on to next station..')        


print(f'Fetched a total of {total_ct} results. Shutting Down..')

