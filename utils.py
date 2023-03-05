import pymongo
import logging
import requests
import datetime
import yaml

from typing import Dict, Sequence

logging.basicConfig(level=logging.INFO)

def get_atlas_string(user: str, password: str, cluster_name: str) -> str:
    return f"mongodb+srv://{user}:{password}@{cluster_name}.lmeahaq.mongodb.net/?retryWrites=true&w=majority"

class NebulaSearch:
    
    EMPLOYEE_LISTING_API_ENDPOINT = 'https://nubela.co/proxycurl/api/linkedin/company/employees/'
    PERSON_PROFILE_API_ENDPOINT = 'https://nubela.co/proxycurl/api/v2/linkedin'

    def __init__(self, credentials: str):
        """
        Initialize Nebula search that will search for LinkedIn companies/ employee profiles and 
        add data to MongoDB

        """
        with open(credentials, 'r') as f:
            self.credentials = yaml.safe_load(f)

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

        # set up MongoDB connection
        atlas_string = get_atlas_string(
            self.credentials['mongodb_user'],
            self.credentials['mongodb_password'],
            self.credentials['mongodb_cluster']
            )
        self.mongodb_client = pymongo.MongoClient(atlas_string)
        
        self.db = self.mongodb_client.biotechs # TODO: should this be parameterized

        self.company_collection = self.db['companies']
        self.employee_collection = self.db['employees']

    def _get_nubela_header(self):
        return {'Authorization': 'Bearer ' + self.credentials['nubela_api_key']}

    def search_employee_listing(
            self, 
            companies: Dict[str, str], 
            employment_status: str = 'current'
            ) -> None:
        """
        Search employee listings for a set of companies.

        """
        for company_name, company_url in companies.items():
            search_params = {
                'company_name': company_name,
                'company_url': company_url
                }

            # check if already cached
            if self.company_collection.find_one(search_params) is not None:
                self.logger.info(f"{company_name} already in collection")
            else:
                # perform search
                params = {
                    'employment_status': employment_status,
                    'url': company_url,
                }

                response = requests.get(
                    self.EMPLOYEE_LISTING_API_ENDPOINT,
                    params=params,
                    headers=self._get_nubela_header()
                    )

                # add information on URL that was searched before adding to database
                data = response.json()
                data.update(search_params)
                data['date_searched'] = datetime.datetime.now()

                self.logger.info(f"{company_name} - fetched data on {len(data['employees'])} employees")
                
                self.company_collection.insert_one(data)

    def search_profile_details(self, profile_urls: Sequence[str]) -> None:
        """
        Search up profile details. Checks if 
        """
        for profile_url in profile_urls:
            search_params = {'profile_url': profile_url}

            if self.employee_collection.find_one(search_params) is None:
                # run search, insert into database
                response = requests.get(
                    self.PERSON_PROFILE_API_ENDPOINT,
                    params={'url': profile_url},
                    headers=self._get_nubela_header()
                    )
                
                data = response.json()

                data.update(search_params)
                data['date_searched'] = datetime.datetime.now()

                self.employee_collection.insert_one(data)
            else:
                # search already run, skip
                self.logger.info(f"URL already in database: {profile_url}")

