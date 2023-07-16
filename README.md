# acblclub-scrap
a scrapper for the acbl live bridge club website. Pulls data from the webpage and moves it to a mariadb

the project needs a db.json file in the settings folder and contain these columns to connect. You need to create the user and the database in your instance of mariadb before running this

{
    "system":"mariadb",
    "user":"username",
    "password":"passwd",
    "host":"",
    "port":3306,
    "database":"bridge_live_results"
}

In the Crawler you will need to update the club URLs "start_urls". Replace the clubs in there with the url from the club you are interested in in the acbl live website. They are currently set for a few
in Calgary Alberta
