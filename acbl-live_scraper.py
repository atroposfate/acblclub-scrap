import scrapy
from scrapy.crawler import CrawlerProcess
import re
import json
import mariadb
import os
import pandas as pd



class DatabasePipeline():
    def __init__(self):
        data_file = 'db.json'
        data_folder = 'settings'
        file_path = os.path.join('acblclub-scrap',data_folder,data_file) 
        with open(file_path,'r') as file:
            self.cred_data = json.load(file)
        self.cur = None
    #allow flexibilty on the database type to be allowed to do this at work and test environment
        if self.cred_data['system'] == 'mariadb':
            try:
                self.conn = mariadb.connect(
                    user=self.cred_data['user'],
                    password=self.cred_data['password'],
                    host=self.cred_data['host'],
                    port=self.cred_data['port'],
                    database=self.cred_data['database']
            )
                print("link to database was created")
                self.cur = self.conn.cursor()
            except mariadb.Error as e:
                print(f"Error connecting to MariaDB")

    def upload_df_to_database(self, df, table_name, prim_key=None):
        sql_columns = ', '.join(df.columns)
        placeholders = ', '.join('?' for column in df.columns)
        update_statements = ', '.join(f'{column} = VALUES({column})' for column in df.columns if column != prim_key)  # Exclude primary key column from updates

        # Iterate over DataFrame rows
        for idx, row in df.iterrows():
            if prim_key:
                sql = f"""
                INSERT INTO player_data ({sql_columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_statements}
                """
            else:
                sql = f"""
                INSERT INTO {table_name} ({sql_columns})
                VALUES ({placeholders})
                """
            try:
                self.cur.execute(sql, tuple(row))
            except mariadb.Error as e:
                #trying to account for the duplicate records
                print(e)

        self.conn.commit()



class ACBL_spider(scrapy.Spider):
    name = 'acbl_club_spider'
    start_urls = ['https://my.acbl.org/club-results/details/817779']
    mydb = DatabasePipeline()

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
                #merged_club_data = {k: v for d in data.get('club') for k, v in d.items()}
                #club_df = df.DataFrame(merged_club_data)
                #print(club_df)
                players_df = self.get_players(data)
                self.mydb.upload_df_to_database(df=players_df,table_name='player_data',prim_key='acbl_num')




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
        player_list = []

        sessions = data['sessions']
        for session_num in range(len(sessions)):
            sections = sessions[session_num]['sections']
            for section_num in range(len(sections)):
                pairs = sections[section_num]['pair_summaries']
                for pair_num in range(len(pairs)):
                    players = pairs[pair_num]['players']
                    for player in players:
                        player_list.append({
                        'name': player['name'],
                        'acbl_num': player['id_number'],
                        'city': player['city'],
                        'state': player['state'],
                        'lifemaster': player['lifemaster'],
                        'master_points': float(player['mp_total']),
                        'bbo_username': player['bbo_username']
                        })



        df = pd.DataFrame(player_list, columns=['name', 'acbl_num', 'city', 'state', 'lifemaster', 'master_points', 'bbo_username'])
        return df








if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ACBL_spider)
    process.start()