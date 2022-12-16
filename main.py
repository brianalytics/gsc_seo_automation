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
##       Insert CSV File to be Read      ##
###########################################  

def read_csv(csv_file):
    orgs_df = pd.read_csv(csv_file)
    return orgs_df

orgs_df = read_csv('file_w_queries.csv')

###########################################
##        Pre-Processsing of CSV         ##
###########################################

def process_orgs_df(orgs_df):
    orgs_df['org_name'] = [i.split('/', 2)[-1] for i in orgs_df['Path']] #remove everything before second '/'
    orgs_df['org_name'] = [i.split('/', 1)[0] for i in orgs_df['org_name']] #remove everything after remaining '/'
    orgs_df['org_name'] = [i.replace('-', ' ') for i in orgs_df['org_name']] #replace dashes in org names with space

    return orgs_df

preprocessed_orgs_df = process_orgs_df(orgs_df)

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
            "startDate": "XXXX-XX-XX", #add a start date here for search, like 2022-01-01
            "endDate": "XXXX-XX-XX", #add an end date here for search, like 2022-03-31
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

gsc_response = [query_gsc(token, keyword, 'https://yourwebsitehere.com/') for keyword in preprocessed_orgs_df['org_name']]

###########################################
##    Post-Processing GSC API Results    ##
###########################################

def post_processing(gsc_response):
    #make a flat list from the list of lists
    flat_list = []
    for sublist in gsc_response: 
        for i in sublist:
            flat_list.append(i)
    
    #flatten list to a single dictionary
    flat_dict = {k: v for i in flat_list for k, v in i.items()}

    #create dataframe from flat_dict result
    kw_df = pd.DataFrame(flat_dict['rows'])

    #change keys from list objects to strings
    kw_df['keys'] = [''.join(i) for i in kw_df['keys']] 

    return kw_df

final_dataframe = post_processing(gsc_response)
final_dataframe #don't forget to export your results in the desired format
