import scrapy
from scrapy.crawler import CrawlerProcess
import re
import json
import mariadb
import os


class ACBL_spider(scrapy.Spider):
    name = 'acbl_club_spider'
    start_urls = ['https://my.acbl.org/club-results/details/817779']

    def start_requests(self):
        headers = {'User-Agent': 'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16'}
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.errback_http, headers=headers)


    def parse(self, response):
        # Check if the response has a successful status code
        if response.status == 200:

            # Extract the JavaScript code containing the 'data' variable
            script_code_match = re.search(r'var data = (.*?);', response.body.decode('utf-8'))
            if script_code_match:
                script_code = script_code_match.group(1)

                # Load the data as a JSON object
                data = json.loads(script_code)

                #scub for the necessary data
                id_value = data.get('id')
                club = data.get('club')
                players = self.get_players(data)



                self.handle_data(id_value,club,players,pairs,session,results)


            else:
                self.logger.error("Unable to find 'data' variable in the response")
        else:
            self.logger.error(f"Received a non-200 status code: {response.status}")

    def errback_http(self, failure):
        # Handle HTTP errors and exceptions
        if failure.check(HttpError):
            response = failure.value.response
            self.logger.error(f"HTTP Error {response.status} occurred for URL: {response.url}")
        elif failure.check(DNSLookupError):
            request = failure.request
            self.logger.error(f"DNS Lookup Error occurred for URL: {request.url}")
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self.logger.error(f"Timeout Error occurred for URL: {request.url}")
        else:
            self.logger.error(f"Error occurred: {failure.getErrorMessage()}")

    def handle_data(self,data):
        pass

    def get_players(self,data):
        player_dict = {}
        names = []
        acbl_nums = []
        city = []
        prov = []
        mp = []
        bbo_username = []
        lifemaster = []

        sessions = data['sessions']
        for session_num in range(len(sessions)):
            sections = sessions[session_num]['sections']
            for section_num in range(len(sections)):
                pairs = sections[section_num]['pair_summaries']
                for pair_num in range(len(pairs)):
                    players = pairs[pair_num]['players']
                    for player in players:
                        names.append((player['name']))
                        acbl_nums.append(player['id_number'])
                        city.append(player['city'])
                        prov.append(player['state'])
                        mp.append(player['mp_total'])
                        bbo_username.append(player['bbo_username'])
                        lifemaster.append(player['lifemaster'])



        player_dict = {'name': names, 'acbl_num': acbl_nums, 'city':city, 'state':prov,'master_points':mp, 'bbo_username':bbo_username, 'lifemaster':lifemaster}



        print(player_dict)


        return player_dict


class DatabasePipeline():
    data_file = 'db.json'
    data_folder = 'settings'
    file_path = os.path.join('acblclub-scrap',data_folder,data_file) #stupid folder is being added
    with open(file_path,'r') as file:
        cred_data = json.load(file)
    cur = None
    #allow flexibilty on the database type to be allowed to do this at work and test environment
    if cred_data['system'] == 'mariadb':
        try:
            conn = mariadb.connect(
                user=cred_data['user'],
                password=cred_data['password'],
                host=cred_data['host'],
                port=cred_data['port'],
                database=cred_data['database']
        )
            print("link to database was created")
            cur = conn.cursor()
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB")







if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ACBL_spider)
    process.start()