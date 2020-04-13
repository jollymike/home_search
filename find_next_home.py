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
MARTA_STATIONS = r'.\data\marta_stations.csv'

total_ct = 0

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

def get_next_pg(next_page_token,backoff = 1):
    API = r'nearbysearch/'
    PARAMS = f"pagetoken={next_page_token}"

    response = get_response(API,PARAMS)
    if response:
        if response.json()['status'] == 'INVALID_REQUEST':
            print('Got an invalid request response. Trying again.')
            backoff *= 2
            sleep(backoff)
            get_next_pg(next_page_token,backoff)
        else:
            return response

def get_details(place_id):
    API = r'details/'
    PARAMS = f"place_id={place_id}&fields=name,rating,formatted_phone_number,formatted_address,url,website,price_level"
    return get_response(API,PARAMS)

def stringClean(string):
    return string.strip().replace(" ", "-").replace("\\", "-").replace(".", "").replace("/", "-")

def process_response(response):
    results = response.json()['results']
    print(f'This station has {len(results)} nearby apartments. Attempting to process..')
    result_list = []
    for result in results:                
        coords_station = (row['lat'],row['lng'])
        coords_apt     = (result['geometry']['location']['lat'],result['geometry']['location']['lng'])                
        apt = {'place_id':result['place_id'],
                'name':result['name'],
                'lat':result['geometry']['location']['lat'],
                'lng':result['geometry']['location']['lng'],
                'nearest_station':row['Station'],
                'distance_to_station':gd.distance(coords_station,coords_apt).miles}                
        sleep(.1) # make sure the calls are nice and slow
        details_res = get_details(result['place_id'])
        if details_res:
            print('got details')
            details = details_res.json()['result']
            result_list.append({**apt,**details})
            global total_ct
            total_ct += 1
        else:
            print('failed to get details')
            result_list.append(apt)
    return result_list

def handle_pages(response,apt_list):
    if 'next_page_token' in response.json():
        print(len(apt_list))
        apt_list += process_response(response)
        print(len(apt_list))
        next_response = get_next_pg(response.json()['next_page_token'])
        print('going to next page')
        return handle_pages(next_response,apt_list)
    else:
        print('last pg')
        print(len(apt_list))
        apt_list += process_response(response)
        print(len(apt_list))
        print(type(apt_list))
        return apt_list

def persist_data(data,station):
    print('Processing for station complete. Persisting to Disk..')
    print(station)
    print(data)
    a = pd.DataFrame(data)
    print(a)
    a.to_csv(stringClean(f'station_{station}_apartments') + '.csv', index=False)

# driver
with open(MARTA_STATIONS) as csv_file:
    csv_reader = csv.DictReader(csv_file, delimiter=',')
    line_count = 0
    print(f'Data loaded..')
    for row in csv_reader:
        station = row['Station']
        print(f'Retrieving apartments near Station {station}..')
        response = get_places(row['lat'],row['lng'],'900','apartment')
        if response:
            h = handle_pages(response,[])
            print(type(h))
            print(h)
            persist_data(h,station)
            print(f'Finished current station. Moving on to next station..')
        else:
            print(f'Request Failed! Moving on to next station..')        

print(f'Fetched a total of {total_ct} results. Shutting Down..')
