import math
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

def request_data_as_df(desired_number_of_rows: int) -> pd.DataFrame():
    """
    This function is in charge of getting data from the
    API: https://fakerapi.it/api/v1/
    and return all the information as a raw pandas DataFrame.

    Args:
        desired_number_of_rows (int): The number of rows that you want to request

    Returns:
        request_df (pd.DataFrame): Information requested from API as a pandas DataFrame
    """

    # Number of rows that have been requested at the runtime
    requested_number_of_rows = 0
    
    # Empty DataFrame created just to append the rows
    request_df = pd.DataFrame()

    # This variable will be useful in the while loop
    number_of_rows_to_request = desired_number_of_rows
    
    # Number of batches that will be used to acquire data
    #   if desired_number_of_rows <= 1000
    batch = 1

    # Number of batches that will be used to acquire data
    #   if desired_number_of_rows > 1000
    total_number_of_batches = math.ceil(desired_number_of_rows/1000)

    if desired_number_of_rows > 1000:
        print(f"""
        Requested: {desired_number_of_rows}
        The domain: https://fakerapi.it/api/v1/, only supports 1000 records
        per request. 
        For this reason you will have to wait: {total_number_of_batches} batches
        To get the desired number of rows
            """)
    
    # All variables above until the next comment are going to be used 
    # for a backoff strategy in case of a request failure
    retry_strategy = Retry(
                            total = 3,
                            status_forcelist = [429, 500, 502, 503, 504],
                            allowed_methods = ["HEAD", "GET", "OPTIONS"],
                            backoff_factor = 5
                        )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)

    
    # Starts a loop to keep getting data from API requests until
    # we have the desired number of rows in the dataframe
    while requested_number_of_rows < desired_number_of_rows:
        try:
            request_response = http.get(f"https://fakerapi.it/api/v1/persons?_quantity={number_of_rows_to_request}")
        except:
            raise Exception(f'Request Timed Out after {retry_strategy.get_backoff_time()}')

        finally:
            http.close()

        # Tranform the request response into a json object
        request_json = request_response.json()
        # Keeps only the key 'data' from the Json created above
        json_data = request_json['data']

        # Transform the json data into a pandas dataframe and normalize the schema
        data_to_append_df = pd.json_normalize(json_data)

        # Updates the counter used to keep the loop running
        number_of_rows_to_request = number_of_rows_to_request - data_to_append_df.count()[0]
        
        # Merging the dataframe created
        request_df = pd.concat([data_to_append_df,request_df])

        # Updates the variable requested_number_of_rows accordingly
        #  with the number of the current rows in the dataframe
        requested_number_of_rows = request_df.count()[0]

        batch = batch + 1

        print(f"""
        Debugging info
            Batch Number: {batch - 1}
            Missing Batches: {total_number_of_batches - (batch - 1)}
            Total Number of Batches: {total_number_of_batches}
            Requested Number of Rows: {requested_number_of_rows}
            Number of rows to request: {number_of_rows_to_request}
        """)

    
    return request_df
