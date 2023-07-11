import scrapy
from scrapy.crawler import CrawlerProcess
import re
import json
import mariadb
import os
import pandas as pd
from datetime import datetime



class DatabasePipeline():
    def __init__(self):
        data_file = 'db.json'
        data_folder = 'settings'
        file_path = os.path.join(data_folder,data_file) 
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

    def upload_df_to_database(self, df, table_name, prim_key=None,date_check=False):

        #this is a little ugly but don't want to update the player table unless it is new information
        sql_columns = ', '.join(df.columns)
        placeholders = ', '.join('?' for column in df.columns)
       
        if date_check:
        # Add condition to only update rows where last updated is older
            update_statements = ', '.join(f'{column} = CASE WHEN VALUES(last_updated) > last_updated THEN VALUES({column}) ELSE {column} END' for column in df.columns if column != prim_key)
        else:
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
    start_urls = ['https://my.acbl.org/club-results/details/813684'] #starting with this just as a sample. Will move up a level
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

                players_df = self.get_players(data)
                club_df = self.get_club(data)
                game_df = self.get_game_details(data)
                self.mydb.upload_df_to_database(df=players_df,table_name='player_data',prim_key='acbl_num', date_check=True)
                self.mydb.upload_df_to_database(df=club_df, table_name='club_data')
                self.mydb.upload_df_to_database(df=game_df, table_name='game_data')


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
        #specific player data
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
                        'bbo_username': player['bbo_username'],
                        'last_updated': datetime.strptime(data['sessions'][0]['game_date'], "%Y-%m-%d %H:%M:%S").date() #used to make sure only the latest is updated
                        })


        print("Building Player Data for game..." + str(data['id']))
        df = pd.DataFrame(player_list, columns=['name', 'acbl_num', 'city', 'state', 'lifemaster', 'master_points', 'bbo_username','last_updated'])
        return df

    def get_club(self,data):
        club_list = []

        club = data['club']
        club_list.append({
            'club_num': club['id'],
            'club_name': club['name'],
            'unit_num': club['unit_no'],
            'district_num': club['district_no'],
            'manager_num': club['manager_no'],
            'alias': club['alias']
        })
        print("Building Club Data")
        df = pd.DataFrame(club_list, columns=['club_num', 'club_name','unit_num','district_num','manager_num','alias'])
        return df


    def get_game_details(self,data):
        game_detail_list = []

        section_count = 0
        for section_num in range(len(data['sessions'])):
            section_count += int(data['sessions'][section_num]['number_of_sections'])

        game_detail_list.append({
            'game_id': data['id'],
            'game_name': data['name'],
            'game_rating': data['rating'],
            'club_num': data['club_id_number'],
            'game_type': data['type'],
            'scoring_method': data['board_scoring_method'],
            'start_date': datetime.strptime(data['start_date'],"%m/%d/%Y").date(),
            'end_date': datetime.strptime(data['end_date'],"%m/%d/%Y").date(),
            'session_cnt': data['number_of_sessions'],
            'section_cnt': section_count
        })

        print("Building Game Data")
        df = pd.DataFrame(game_detail_list, columns=['game_id', 'game_name', 'game_rating','club_num','game_type','scoring_method','start_date','end_date','session_cnt','section_cnt'])
        return df

    def get_section_data(self,data):
    #collapsing the session and section data into one table
        pass

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ACBL_spider)
    process.start()