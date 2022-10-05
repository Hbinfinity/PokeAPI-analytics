import requests
import json
from typing import List, Dict
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests_toolbelt import sessions
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession

class APIIngestor():
    '''
    Extendable utility class for calling any API endpoint, currently the GET portion is implemented.
    Optional request parameters can be passed a dictionary like -> {'to' : '2022-01-01', 'from' : '2022-01-02'}
    '''
    def __init__(self, base_url:str, request_parameters: Dict[str,str] = None):
        self.__http_session = None
        self.base_url = base_url
        self.request_parameters = request_parameters if request_parameters else []
    
    def set_HTTP_adapter(self, max_retry: int, status_list: List[str], backoff_constant: int) -> None:
        '''
        Configure the HTTP Adapter, set the retry strategy with exponential back-off and mount on the endpoints.
        An optimal configuration for exponential backoff with retries is given as:
            * max_retry = 4 # The maximum number of retries.
            * status_list = [429, 500, 502, 503, 504] # The HTTP responses to retry on, 500 errors are server-side related. 429 is 'too many requests' 
            * backoff_constant = 2 # How long to wait in between retries 
        '''
        # number of waiting secs between requests = {backoff factor} * (2 ** ({number of total retries} - 1)) 
        retry_strategy = Retry(
            total = max_retry,
            status_forcelist = status_list,
            backoff_factor = backoff_constant
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        
        http_session = sessions.BaseUrlSession(base_url= self.base_url)

        # Mount the adapter on both http and https urls. 
        http_session.mount("https://", adapter)
        http_session.mount("http://", adapter)
        self.__http_session = http_session
        
    def reset_session(self) -> None:
        '''
        Resets the instance variables and closes the session. This needs to be called for reuse of the class with different arguments.
        '''
        self.params = []
        self.base_url = ''
        self.__http_session.close()
        
        print('Session cleared')

    def send_get_request(self, path = None):
        '''
        Sends the GET API requests with parameters and the mounted HTTP Adapter. 
        TODO add functionality to handle different authentication types.
        '''
        HTTP_session = self.__http_session
        
        if path: endpoint_url = self.base_url + path
        else: endpoint_url = self.base_url
        
        request_parameters = self.request_parameters
        
        return HTTP_session.get(endpoint_url, params = request_parameters)
    
    
    def write_to_file(self, json_array: List[object], target_bucket:str, file_name:str, file_format:str) -> None:
        '''
        Writes the ingestion list of json into a file.
        :params json_array, target_bucket, file_format
        :return Void
        '''
        sc = SparkContext.getOrCreate()
        spark = SparkSession(sc)
        df = spark.read.option('multiline','true').json(sc.parallelize(json_array))
      
        # Repartition to get rid of small files, overwrite for the same extraction date to enforce idempotency.
        df.coalesce(1)\
                    .write\
                    .format(file_format)\
                    .mode('overwrite')\
                    .save(f'{target_bucket}/{file_name}.{file_format}')

        # print(f'completed extraction for data on : {datetime_path}')