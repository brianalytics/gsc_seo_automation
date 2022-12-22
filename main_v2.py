#####################################################
##                 Imports                         ##
#####################################################
import pandas as pd
import requests
import json


########################################
##       Org API Link Formation       ##
########################################

def link_formation(csv_file):
    data = pd.read_csv(csv_file)

    data['COL_PERMALINK'] = [i.split('/', 2)[-1] for i in data['Path']]
    data['COL_PERMALINK'] = [i.split('/', 1)[0] for i in data['COL_PERMALINK']]

    org_permalinks = []
    for permalink in data['COL_PERMALINK']:
        org_permalinks.append(permalink)

    req_urls = []
    for permalink in org_permalinks:
        req_urls.append([f'https://api.crunchbase.com/api/v4/entities/organizations/{permalink}?field_ids=name&user_key=userKey' for permalink in org_permalinks])

    flat_urls = []
    for sublist in req_urls:
        for i in sublist:
            flat_urls.append(i)

        return flat_urls

flat_urls = link_formation('csv_file_path.csv')


########################################
##      Crunchbase API Requests       ##
########################################

def api_extraction(flat_urls):
    response_data = []
    for url in flat_urls:
        r = requests.get(url)
        rjson = r.json()
        response_data.append(rjson)

        df = pd.json_normalize(response_data)
        df.rename(columns={'properties.name': 'org_name'}, inplace=True)
    
    return df

df = api_extraction(flat_urls)

###########################################
##           Transform Results           ##
###########################################

#create function for transformations

df = df.drop(columns=['error', 'code'])
df = df.dropna()


#####################################################
##              Refresh Access Token               ##
#####################################################

def refresh_token(clientID, clientSecret, refreshToken):
    url = "https://oauth2.googleapis.com/token"
    payload = f'client_id={clientID}&client_secret={clientSecret}&grant_type=refresh_token&refresh_token={refreshToken}'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    refreshed_token = json.loads(response.text)["access_token"]
    
    return refreshed_token

with open('secrets_file.json') as f:
    secrets = json.load(f)
    clientID = secrets['web']['client_id']
    clientSecret = secrets['web']['client_secret']
    refreshToken = secrets['web']['refresh_token']
    token = refresh_token(clientID, clientSecret, refreshToken)
    
    
###########################################
##  Make Request to GSC API for Each Org ##
###########################################

keywords_response = []

def query_gsc(accessToken, keyword, site_url):
    url = f'https://www.googleapis.com/webmasters/v3/sites/siteUrl/searchAnalytics/query?siteUrl={site_url}&access_token={accessToken}'
    headers = {
    'Content-Type': 'application/json'
    }

    payload = json.dumps({
            "startDate": "XXXX-XX-XX",
            "endDate": "XXXX-XX-XX",
            "type": "web",
            "dimensions": [
                "QUERY"
            ],
            "dimensionFilterGroups": [
                {
                    "filters": [
                        {
                            "dimension": "QUERY",
                            "operator": "CONTAINS",
                            "expression": keyword
                        }
                    ]
                }
            ]
        })
    
    r = requests.request("POST", url, headers=headers, data=payload)
    response_data = r.json()
    keywords_response.append(response_data)
    return keywords_response

gsc_response = [query_gsc(token, keyword, 'https://www.websiteurl.com/') for keyword in df['org_name']]



###########################################
##    Post-Processing GSC API Results    ##
###########################################

###create function with code portions below

#def post_processing(gsc_response):
  
interim_df = []
for i in keywords_response:
    interim_df.append(i['rows'])
    
flat_list = []
for sublist in interim_df: 
    for i in sublist:
        flat_list.append(i)
        
test_df = pd.DataFrame(flat_list)

test_df['keys'] = [''.join(i) for i in test_df['keys']]

test_df['keyword'] = [i.split(' ', 1)[-1] for i in test_df['keys']]

#export results to csv
test_df.to_csv('output_csv.csv')

#can conduct further analysis from here to see the top keywords driving traffic to your website
