'''A scrapper of the ACBL live club website and storing the data in a mariadb table for later use. Tables have to be created to match the scraper
the credentials for the database are stored in a setting folder db.json, long term goal is to build some analyics on individual performance 
This is only working for pairs and not team games'''


import scrapy
from scrapy.crawler import CrawlerProcess
import re
import json
import mariadb
import os
import pandas as pd
from datetime import datetime
import random



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
    start_urls = ['https://my.acbl.org/club-results/261750'] #This is crawling through a couple clubs. Should be adding specific clubs to this
    mydb = DatabasePipeline()
    headerlist = [
        {'User-Agent': 'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16'},
        {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'},
        {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'},
        ]


    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.errback_http, headers=self.headerlist[0])


    def parse(self, response):
        #Going through the clubs for only pairs
        for row in response.xpath('//tr[td[text()="PAIRS"]]'):
            # For each such row, find the "Results" link and follow it
            headers = random.choice(self.headerlist)
            result_link = row.xpath('.//a[contains(text(), "Results")]/@href').get()
            if result_link:
                yield response.follow(result_link, self.parse_result_page, headers=headers)



    def parse_result_page(self, response):
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
                section_df = self.get_section_data(data)
                hand_record = self.get_hand_records(data) #this returns a dictionary for 2 tables
                hand_results = self.get_hand_results(data)
                #overall_results = self.get_overall_results(data)
                self.mydb.upload_df_to_database(df=players_df,table_name='player_data',prim_key='acbl_num', date_check=True)
                self.mydb.upload_df_to_database(df=club_df, table_name='club_data')
                self.mydb.upload_df_to_database(df=game_df, table_name='game_data')
                self.mydb.upload_df_to_database(df=section_df, table_name='section_data')
                self.mydb.upload_df_to_database(df=hand_record['hand_record'], table_name='hand_records_data')
                self.mydb.upload_df_to_database(df=hand_record['hand_expect'], table_name='hand_possibility_data')
                self.mydb.upload_df_to_database(df=hand_results, table_name='hand_results_data')


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
                        'master_points': float(player['mp_total']) if player['mp_total'] is not None else 0.0,
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
        section_detail_list = []
        sessions = data['sessions']

        for session_num in range(len(sessions)):
            sections = sessions[session_num]['sections']
            hand_record_id = sessions[session_num]['hand_record_id']
            game_id = sessions[session_num]['event_id']
            for section_num in range(len(sections)):
                section_detail_list.append({
                    'section_id': sections[section_num]['id'],
                    'game_id': game_id,
                    'session_id': sections[section_num]['session_id'], #usually the same as the game_id
                    'section_name': sections[section_num]['name'],
                    'hand_record': hand_record_id,
                    'boards_per': sections[section_num]['boards_per_round'],
                    'round_count': sections[section_num]['number_of_rounds'],
                    'pair_count': len(sections[section_num]['pair_summaries'])    
                }
                )
        print("Building Section Data")
        df = pd.DataFrame(section_detail_list,columns=['section_id','game_id','session_id','section_name','hand_record','boards_per','round_count','pair_count'])
        return df
    
    def get_hand_records(self,data):
        hand_record_details = []
        hand_expectation = []
        sessions = data['sessions']
        
        for session_num in range(len(sessions)):
            session_id = sessions[session_num]['id']
            hand_records = sessions[session_num]['hand_records']
            hand_record_id = sessions[session_num]['hand_record_id']
            for hand_num in range(len(hand_records)):
                board_num_id = None
                #get board id from the board results
                for board_num in range(len(sessions[session_num]['sections'][0]['boards'])):
                    if hand_records[hand_num]['board'] == sessions[session_num]['sections'][0]['boards'][board_num]['board_number']:
                        board_num_id = sessions[session_num]['sections'][0]['boards'][board_num]['id']
                hand_record_details.append({
                    'id': hand_records[hand_num]['id'],
                    'hand_record': hand_record_id,
                    'board': hand_records[hand_num]['board'],
                    'board_id_num': board_num_id,
                    'direction': 'N',
                    'spades': hand_records[hand_num]['north_spades'],
                    'hearts': hand_records[hand_num]['north_hearts'],
                    'diamonds': hand_records[hand_num]['north_diamonds'],
                    'clubs': hand_records[hand_num]['north_clubs'],
                })
                hand_record_details.append({
                    'id': hand_records[hand_num]['id'],
                    'hand_record': hand_record_id,
                    'board': hand_records[hand_num]['board'],
                    'board_id_num': board_num_id,
                    'direction': 'S',
                    'spades': hand_records[hand_num]['south_spades'],
                    'hearts': hand_records[hand_num]['south_hearts'],
                    'diamonds': hand_records[hand_num]['south_diamonds'],
                    'clubs': hand_records[hand_num]['south_clubs'],
                })
                hand_record_details.append({
                    'id': hand_records[hand_num]['id'],
                    'hand_record': hand_record_id,
                    'board': hand_records[hand_num]['board'],
                    'board_id_num': board_num_id,
                    'direction': 'E',
                    'spades': hand_records[hand_num]['east_spades'],
                    'hearts': hand_records[hand_num]['east_hearts'],
                    'diamonds': hand_records[hand_num]['east_diamonds'],
                    'clubs': hand_records[hand_num]['east_clubs'],
                })
                hand_record_details.append({
                    'id': hand_records[hand_num]['id'],
                    'hand_record': hand_record_id,
                    'board': hand_records[hand_num]['board'],
                    'board_id_num': board_num_id,
                    'direction': 'W',
                    'spades': hand_records[hand_num]['west_spades'],
                    'hearts': hand_records[hand_num]['west_hearts'],
                    'diamonds': hand_records[hand_num]['west_diamonds'],
                    'clubs': hand_records[hand_num]['west_clubs'],
                })
                hand_expectation.append({
                    'id': hand_records[hand_num]['id'],
                    'hand_record': hand_record_id,
                    'board': hand_records[hand_num]['board'],
                    'board_id_num': board_num_id,
                    'dealer':hand_records[hand_num]['dealer'],
                    'vulnerability':hand_records[hand_num]['vulnerability'],
                    'double_dummy_ew':hand_records[hand_num]['double_dummy_ew'],
                    'double_dummy_ns':hand_records[hand_num]['double_dummy_ns'],
                    'par':hand_records[hand_num]['par']

                })

        df_hr = pd.DataFrame(hand_record_details,columns=['id','hand_record','board','board_id_num','direction','spades','hearts','diamonds','clubs'])
        #reformat to get better data, remove spaces and turn 10 into T so it is only one character
        df_hr['spades'] = df_hr['spades'].str.replace('10', 'T').str.replace(' ', '')
        df_hr['hearts'] = df_hr['hearts'].str.replace('10', 'T').str.replace(' ', '')
        df_hr['diamonds'] = df_hr['diamonds'].str.replace('10', 'T').str.replace(' ', '')
        df_hr['clubs'] = df_hr['clubs'].str.replace('10', 'T').str.replace(' ', '')
        
        df_hr['board_id_num'] = df_hr['board_id_num'].fillna(0)

        df_hexp = pd.DataFrame(hand_expectation, columns=['id','hand_record','board','board_id_num','dealer','vulnerability','double_dummy_ew','double_dummy_ns','par'])
        df_hexp['board_id_num'] = df_hexp['board_id_num'].fillna(0)
        return {'hand_record':df_hr,'hand_expect':df_hexp}


    def get_overall_results(self,data):
        pass

    def get_hand_results(self,data):
        board_results_details = []
        sessions = data['sessions']
        add_pair_direction = False
        
        for session_num in range(len(sessions)):
            session_id = sessions[session_num]['id']
            hand_record_id = sessions[session_num]['hand_record_id']  
            sections = sessions[session_num]['sections']
            for section_num in range(len(sections)):
                section_id = sections[section_num]['id']
                boards = sections[section_num]['boards']
                if sections[section_num]['pair_summaries'][0]['direction']:
                    add_pair_direction = True
                else:
                    add_pair_direction = False
                for board_num in range(len(boards)):
                    results = boards[board_num]['board_results']
                    board_num = boards[board_num]['board_number']
                    for result_num in range(len(results)):
                        if add_pair_direction:
                            ns_pair = results[result_num]['ns_pair'] + 'NS'
                            ew_pair = results[result_num]['ew_pair'] + 'EW'
                        else:
                            ns_pair = results[result_num]['ns_pair'] 
                            ew_pair = results[result_num]['ew_pair']

                        board_results_details.append({
                            'result_id': results[result_num]['id'],
                            'session_id':session_id,
                            'hand_record':hand_record_id,
                            'section_id':section_id,
                            'board_id':results[result_num]['board_id'],
                            'board_num': board_num,
                            'round': results[result_num]['round_number'],
                            'table_num': results[result_num]['table_number'],
                            'ns_pair':ns_pair,
                            'ew_pair':ew_pair,
                            'ns_score':results[result_num]['ns_score'],
                            'ew_score':results[result_num]['ew_score'],
                            'contract':results[result_num]['contract'],
                            'declarer':results[result_num]['declarer'],
                            'ew_match_points':results[result_num]['ew_match_points'],
                            'ns_match_points':results[result_num]['ns_match_points'],
                            'opening_lead':results[result_num]['opening_lead'],
                            'result':results[result_num]['result'],
                            'tricks_taken':results[result_num]['tricks_taken']

                        })

        df = pd.DataFrame(board_results_details,columns=['result_id','session_id','hand_record','section_id','board_id','board_num','round','table_num','ns_pair','ew_pair','ns_score','ew_score','contract','declarer','ew_match_points','ns_match_points','opening_lead','result','tricks_taken'])
        df['result'] = df['result'].str.replace('=', '0')
        #shoudl be some nulls in results when there are weird adjustments
        df['ns_score'] = df['ns_score'].str.replace('PASS','0')
        df['ew_score'] = df['ew_score'].str.replace('PASS','0')
        return df

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(ACBL_spider)
    process.start()