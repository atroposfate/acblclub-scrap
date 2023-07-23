'''A scrapper of the ACBL live club website and storing the data in a mariadb table for later use. Tables will be created if they don't exist
The credentials and database created for this project have to be in the settings folder in a file called db.json,
this is the structure of the db.json:
 {
    "system":"mariadb",
    "user":"username",
    "password":"password",
    "host":"xxx.xxx.xxx.xxx",
    "port":3306,
    "database":"bridge-results"
}
The hope is to use this do some analytics on the actual results and database

This is only working for pairs and not team games and the tag on the ACBL website expects it to be flagged as 'PAIRS'''


import scrapy
from scrapy.crawler import CrawlerProcess
import re
import json
import mariadb
import os
import pandas as pd
from datetime import datetime, timedelta
import random
import numpy as np



class DatabasePipeline():

    table_list = ('player_data','club_data','game_data','section_data','hand_records_data','hand_possibility_data','hand_results_data','pair_results_data','strat_result_summary_data')
    
    def __init__(self):
        data_file = 'db.json'
        data_folder = 'settings'
        app_folder = 'acblclub-scrap'
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
        
        for table in self.table_list:
            if self.table_exists(table):
                continue
            else:
                print('Building non-existant table' + table)
                self.build_table(table)

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
    
    def purge_db_contents(self):
        for table in self.table_list:
            sql = f'DELETE FROM {table};'
            self.cur.execute(sql)
        
        self.conn.commit()

    def table_exists(self,table_name):

        self.cur.execute(f'''
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = '{table_name}'
        ''')
        if self.cur.fetchone()[0] == 1:
            return True

        return False


    def get_game_list(self):
        sql = f'SELECT `game_id` FROM `game_data` GROUP BY `game_id`'
        self.cur.execute(sql)
        results = self.cur.fetchall()
        game_id = [int(result[0]) for result in results]

        return game_id

    def build_table(self,table_name):
        sql = f'CREATE TABLE IF NOT EXISTS {table_name}'

        if table_name == 'club_data':
            sql += ' (`club_num` int(7) NOT NULL,`club_name` varchar(45) NOT NULL,`unit_num` smallint(6) NOT NULL,`district_num` smallint(6) NOT NULL,`manager_num` int(10) DEFAULT NULL,`alias` varchar(10) DEFAULT NULL, PRIMARY KEY (`emp_no`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        elif table_name == 'game_data':
            sql += ' (`game_id` int(7) NOT NULL,`game_name` varchar(90) DEFAULT NULL,`game_rating` tinyint(4) NOT NULL,`club_num` int(7) NOT NULL,`game_type` varchar(15) NOT NULL,`scoring_method` varchar(15) NOT NULL,`start_date` date NOT NULL,`end_date` date DEFAULT NULL,`session_cnt` tinyint(4) NOT NULL,`section_cnt` tinyint(4) NOT NULL, PRIMARY KEY (`game_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        elif table_name == 'hand_possibility_data':
            sql += ' (`hand_id` int(11) NOT NULL,`hand_record` varchar(10) NOT NULL,`board` tinyint(4) NOT NULL,`board_id_num` int(15) DEFAULT NULL,`dealer` varchar(1) NOT NULL,`vulnerability` varchar(10) NOT NULL,`double_dummy_ew` varchar(40) NOT NULL,`double_dummy_ns` varchar(45) NOT NULL,`par` varchar(40) NOT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        elif table_name == 'hand_records_data':
            sql += ' (`hand_id` int(11) NOT NULL,`hand_record` varchar(10) NOT NULL,`board` tinyint(4) NOT NULL,`board_id_num` int(15) DEFAULT NULL,`direction` varchar(1) NOT NULL,`spades` varchar(13) NOT NULL,`hearts` varchar(13) NOT NULL,`diamonds` varchar(13) NOT NULL,`clubs` varchar(13) NOT NULL,PRIMARY KEY (`id`,`direction`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        elif table_name == 'hand_results_data':
            sql += ' (`result_id` int(15) NOT NULL,`session_id` int(7) NOT NULL,`hand_record` varchar(9) NOT NULL,`section_id` int(7) NOT NULL,`board_id` int(15) NOT NULL,`board_num` tinyint(4) NOT NULL,`round_num` tinyint(4) NOT NULL,`table_num` tinyint(4) NOT NULL,`ns_pair` varchar(5) NOT NULL,`ew_pair` varchar(5) NOT NULL,`ns_score` int(6) NOT NULL,`ew_score` int(6) NOT NULL,`contract` varchar(10) DEFAULT NULL,`declarer` varchar(1) DEFAULT NULL,`ew_match_points` decimal(6,2) NOT NULL,`ns_match_points` decimal(6,2) NOT NULL,`opening_lead` varchar(4) DEFAULT NULL,`result` tinyint(4) DEFAULT NULL,`tricks_taken` tinyint(4) DEFAULT NULL,PRIMARY KEY (`result_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        elif table_name == 'pair_results_data':
            sql += ' (pair_id_num int(15) NOT NULL,session_id int(7) NOT NULL,section_id int(7) NOT NULL,acbl_num int(10) NOT NULL,pair varchar(6) NOT NULL,score decimal(6,2) NOT NULL,percentage decimal(6,2) NOT NULL,mp_earned decimal(5,2) DEFAULT NULL,direction varchar(2) DEFAULT NULL,PRIMARY KEY (pair_id_num,acbl_num)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        elif table_name == 'player_data':
            sql += ' (`acbl_num` int(10) NOT NULL,`name` varchar(25) DEFAULT NULL,`city` varchar(20) DEFAULT NULL,`state` varchar(20) DEFAULT NULL,`master_points` float DEFAULT NULL,`bbo_username` varchar(20) DEFAULT NULL,`lifemaster` tinyint(1) NOT NULL,`last_updated` date NOT NULL,PRIMARY KEY (`acbl_num`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        elif table_name == 'section_data':
            sql += '(`section_id` int(7) NOT NULL,`section_name` varchar(10) DEFAULT NULL,`game_id` int(7) NOT NULL,`session_id` int(7) NOT NULL,`hand_record` varchar(10) DEFAULT NULL,`boards_per` tinyint(4) DEFAULT NULL,`round_count` tinyint(4) DEFAULT NULL,`pair_count` smallint(6) DEFAULT NULL, PRIMARY KEY (`section_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        elif table_name == 'strat_result_summary_data':
            sql += ' (`strat_id` int(15) NOT NULL,`pair_id_num` int(15) NOT NULL,`strat_num` smallint(6) NOT NULL,`rank` tinyint(4) DEFAULT NULL,`strat_type` varchar(10) DEFAULT NULL,PRIMARY KEY (`strat_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;'
        else:
            print('Table has no definitiion')
            sql = None
        
        try:
            if sql:
                self.cur.execute(sql)
            else:
                print('SQL is NoneType')
        except Exception as e:
            print(e)


class ACBL_spider(scrapy.Spider):
    name = 'acbl_club_spider'
    #go to the acbl live clubs site and find the clubs you want to follow and place their ID here
    start_urls = ['https://my.acbl.org/club-results/261750','https://my.acbl.org/club-results/275149','https://my.acbl.org/club-results/276287','https://my.acbl.org/club-results/273540','https://my.acbl.org/club-results/264820'] #This is crawling through a couple clubs. Should be adding specific clubs to this
    mydb = DatabasePipeline()
    already_pulled = mydb.get_game_list()
    headerlist = [
        {'User-Agent': 'Opera/9.80 (X11; Linux i686; Ubuntu/14.10) Presto/2.12.388 Version/12.16'},
        {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9'},
        {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36'},
        ]
    max_data_age = 14
    current_date = datetime.now()

    def __init__(self, date_limit=False, *args, **kwargs):
        super(ACBL_spider, self).__init__(*args, **kwargs)
        self.date_limit = date_limit

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, errback=self.errback_http, headers=self.headerlist[0])


    def parse(self, response):
        #Going through the clubs for only pairs
        
        for row in response.xpath('//tr[td[text()="PAIRS"]]'):
            #adding a check for date limit so it doesn't come through everything again
            if self.date_limit:
                date_str = row.xpath('td/@data-sort').get()
                event_date = datetime.fromtimestamp(int(date_str))
                age_in_days = (self.current_date - event_date).days
                if age_in_days > self.max_data_age:
                    print("found old record "+ str(age_in_days))
                    break
            game_not_found = True
            headers = random.choice(self.headerlist)
            result_link = row.xpath('.//a[contains(text(), "Results")]/@href').get()
            web_game_id = int(result_link.split('/')[-1])
            #find duplicate games and skip the work of finding it
            for game in self.already_pulled:
                if game == web_game_id:
                    game_not_found = False

            if result_link and game_not_found:
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
                id_value = int(data.get('id'))
                skip = False
                if self.already_pulled:
                    for game in self.already_pulled:
                        if id_value == game:
                            skip = True
                if not skip:
                    self.add_data(data)


                

            else:
                self.logger.error("Unable to find 'data' variable in the response")
        else:
            self.logger.error(f"Received a non-200 status code: {response.status}")

    def add_data(self,data):
        players_df = self.get_players(data)
        club_df = self.get_club(data)
        game_df = self.get_game_details(data)
        section_df = self.get_section_data(data)
        hand_record = self.get_hand_records(data) #this returns a dictionary for 2 tables
        hand_results = self.get_hand_results(data)
        game_results = self.get_game_results(data)
        score_summary = self.get_score_summary(data)
        self.mydb.upload_df_to_database(df=players_df,table_name='player_data',prim_key='acbl_num', date_check=True)
        self.mydb.upload_df_to_database(df=club_df, table_name='club_data')
        self.mydb.upload_df_to_database(df=game_df, table_name='game_data')
        self.mydb.upload_df_to_database(df=section_df, table_name='section_data')
        self.mydb.upload_df_to_database(df=hand_record['hand_record'], table_name='hand_records_data')
        self.mydb.upload_df_to_database(df=hand_record['hand_expect'], table_name='hand_possibility_data')
        self.mydb.upload_df_to_database(df=hand_results, table_name='hand_results_data')
        self.mydb.upload_df_to_database(df=game_results, table_name='pair_results_data')
        self.mydb.upload_df_to_database(df=score_summary, table_name='strat_result_summary_data')

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
        df['lifemaster'] = df['lifemaster'].fillna(0)
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
            for section_num in range(len(sections)):
                section_detail_list.append({
                    'section_id': sections[section_num]['id'],
                    'game_id': data['id'],
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
                    'hand_id': hand_records[hand_num]['id'],
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
                    'hand_id': hand_records[hand_num]['id'],
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
                    'hand_id': hand_records[hand_num]['id'],
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
                    'hand_id': hand_records[hand_num]['id'],
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
                    'hand_id': hand_records[hand_num]['id'],
                    'hand_record': hand_record_id,
                    'board': hand_records[hand_num]['board'],
                    'board_id_num': board_num_id,
                    'dealer':hand_records[hand_num]['dealer'],
                    'vulnerability':hand_records[hand_num]['vulnerability'],
                    'double_dummy_ew':hand_records[hand_num]['double_dummy_ew'],
                    'double_dummy_ns':hand_records[hand_num]['double_dummy_ns'],
                    'par':hand_records[hand_num]['par']

                })

        df_hr = pd.DataFrame(hand_record_details,columns=['hand_id','hand_record','board','board_id_num','direction','spades','hearts','diamonds','clubs'])
        #reformat to get better data, remove spaces and turn 10 into T so it is only one character
        df_hr['spades'] = df_hr['spades'].str.replace('10', 'T').str.replace(' ', '')
        df_hr['hearts'] = df_hr['hearts'].str.replace('10', 'T').str.replace(' ', '')
        df_hr['diamonds'] = df_hr['diamonds'].str.replace('10', 'T').str.replace(' ', '')
        df_hr['clubs'] = df_hr['clubs'].str.replace('10', 'T').str.replace(' ', '')
        
        df_hr['board_id_num'] = df_hr['board_id_num'].fillna(0)
        df_hr = df_hr.dropna(subset=['hand_record'])
        df_hr = df_hr[df_hr['hand_record'] != 'SHUFFLE']

        df_hexp = pd.DataFrame(hand_expectation, columns=['hand_id','hand_record','board','board_id_num','dealer','vulnerability','double_dummy_ew','double_dummy_ns','par'])
        df_hexp['board_id_num'] = df_hexp['board_id_num'].fillna(0)
        df_hexp = df_hexp.dropna(subset=['hand_record'])
        df_hexp = df_hexp[df_hexp['hand_record'] != 'SHUFFLE']
        return {'hand_record':df_hr,'hand_expect':df_hexp}


    def get_game_results(self,data):
        game_results_details = []
        sessions = data['sessions']
        add_pair_direction = False

        for session_num in range(len(sessions)):
            session_id = sessions[session_num]['id']
            sections = sessions[session_num]['sections']
            for section_num in range(len(sections)):
                section_id = sections[section_num]['id']
                pair_summaries = sections[section_num]['pair_summaries']
                if sections[section_num]['pair_summaries'][0]['direction']:
                    add_pair_direction = True
                else:
                    add_pair_direction = False
                for pair_num in range(len(pair_summaries)):
                    if add_pair_direction:
                        pair = pair_summaries[pair_num]['pair_number'] + pair_summaries[pair_num]['direction']
                    else:
                        pair = pair_summaries[pair_num]['pair_number']
                    pair_summary_id = pair_summaries[pair_num]['id']
                    score = pair_summaries[pair_num]['score']
                    percentage = pair_summaries[pair_num]['percentage']
                    players = pair_summaries[pair_num]['players']
                    for player_num in range(len(players)):
                        direction = None
                        if len(players[player_num]['awards_score'])> 0:
                            mp = players[player_num]['awards_score'][0]['total']
                        else:
                            mp = None
                        
                        #set default direction for the pair
                        if add_pair_direction:
                            if pair_summaries[pair_num]['direction'] == 'NS':
                                if pair_num == 0:
                                    direction = 'N'
                                else:
                                    direction = 'S'
                            else:
                                if pair_num != 0:
                                    direction = 'E'
                                else:
                                    direction = 'W'
                        else:
                            direction = None

                        game_results_details.append({
                            'pair_id_num': pair_summary_id,
                            'session_id': session_id,
                            'section_id':section_id,
                            'acbl_num': players[player_num]['id_number'],
                            'pair': pair,
                            'score': score,
                            'percentage': percentage,
                            'mp_earned': mp,
                            'direction': direction
                        })
        df = pd.DataFrame(game_results_details,columns=['pair_id_num','session_id','section_id','acbl_num','pair','score','percentage','mp_earned','direction'])
        #add some random easy identifiable numbers if there is no acbl number
        df['percentage'] = df['percentage'].fillna(0)
        return df

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
                            'round_num': results[result_num]['round_number'],
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

        df = pd.DataFrame(board_results_details,columns=['result_id','session_id','hand_record','section_id','board_id','board_num','round_num','table_num','ns_pair','ew_pair','ns_score','ew_score','contract','declarer','ew_match_points','ns_match_points','opening_lead','result','tricks_taken'])
        df['result'] = df['result'].str.replace('=', '0')
        #hasn't been tested
        df.replace('', None, inplace=True)
        #shoudl be some nulls in results when there are weird adjustments
        df['ns_score'] = df['ns_score'].str.replace('PASS','0')
        df['ew_score'] = df['ew_score'].str.replace('PASS','0')
        df = df.dropna(subset=['round_num','hand_record'])
        df = df[df['hand_record'] != 'SHUFFLE']
        return df

    def get_score_summary(self,data):
        score_results_details = []
        sessions = data['sessions']

        for session_num in range(len(sessions)):
            sections = sessions[session_num]['sections']
            for section_num in range(len(sections)):
                pair_summaries = sections[section_num]['pair_summaries']
                for pair_num in range(len(pair_summaries)):
                    pair_summary_id = pair_summaries[pair_num]['id']
                    strats = pair_summaries[pair_num]['strat_place']
                    for strat_num in range(len(strats)):
                        score_results_details.append({
                            'strat_id':strats[strat_num]['id'],
                            'pair_id_num':pair_summary_id,
                            'strat_num':strats[strat_num]['strat_number'],
                            'rank':strats[strat_num]['rank'],
                            'strat_type':strats[strat_num]['type']
                        })
        df = pd.DataFrame(score_results_details,columns=['strat_id','pair_id_num','strat_num','rank','strat_type'])
        return df


if __name__ == "__main__":
    
    #data = DatabasePipeline()
    #data.purge_db_contents() #don't want this in place all the time
    process = CrawlerProcess()
    process.crawl(ACBL_spider,date_limit=True)
    process.start()