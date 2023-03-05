### DESCRIPTION #########################################################################
# Pull company data from LinkedIn, using Nubela
#

### PREAMBLE ############################################################################
import yaml

from utils import NebulaSearch

with open('config.yml', 'r') as f:
    config = yaml.safe_load(f)

### MAIN ################################################################################

search = NebulaSearch('.credentials.yml')

search.search_employee_listing(config['companies'], config['employment_status'])

# search for employee information on all the 
for company in search.company_collection.find():
    employee_urls = [ e['profile_url'] for e in company['employees'] ]
    search.search_profile_details(employee_urls)