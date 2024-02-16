######################################### pwhl-pbp-scraper.py ######################################
#                                                                                                      #
#                                      Name: PWHL play-by-play scraper                                 #
#                                         V: 1.0.1                                                     #
#                                V. Updates: Init                                                      #
#                                       Dev: Zach Andrews                                              #
#                                     About: A python package to scrape                                #
#                                           PWHL data                                                  #
#                                                                                                      #
#                                                                                                      #
########################################################################################################

######################################### Import Modules ###############################################
import pandas as pd
import requests
import numpy as np
import json
############################################# Config ###################################################
# Function to scrape single game
def scrape_game(game_id):
    print("Scraping game {}...".format(game_id))
    # Make request. URL is https://lscluster.hockeytech.com/feed/index.php?feed=statviewfeed&view=gameCenterPlayByPlay&game_id={id}&key=694cfeed58c932ee&client_code=pwhl&lang=en&league_id=&callback=angular.callbacks._8
    try:
        req = requests.get("https://lscluster.hockeytech.com/feed/index.php?feed=statviewfeed&view=gameCenterPlayByPlay&game_id={}&key=694cfeed58c932ee&client_code=pwhl&lang=en&league_id=&callback=angular.callbacks._8".format(game_id))  
        req.raise_for_status() 
    # Handle HTTP errors
    except requests.exceptions.HTTPError as http_err:
        print(f"Play-by-Play API HTTP error occurred: {http_err}")
    # Handle any exception related to the request
    except requests.exceptions.RequestException as req_exc:
        print(f"Play-by-Play API request failed: {req_exc}")
    # Handle value-related issues
    except ValueError as val_err:
        print(f"Play-by-Play API Value error occured: {val_err}")
    else:
        pbp_text = req.text 
        pbp = extract_df(pbp_text)
        pbp = clean_pbp(pbp)
        print("Game {} finished.\n".format(game_id))
    return pbp

def extract_df(pbp_text):
    # Need to strip out the angular callbacks tag
    json_str = pbp_text.strip('angular.callbacks._8(').rstrip(');')
    # Parse the JSON string
    pbp_json = json.loads(json_str)
    pbp_df = pd.json_normalize(pbp_json)
    return pbp_df

def clean_pbp(pbp):
    pbp = clean_players(pbp)
    return pbp

def clean_players(pbp):
    # shot, block, shootout primary
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='shot')|(pbp['event']=='shootout'),'event_primary_player_name'] = pbp['details.shooter.firstName'] + ' ' +pbp['details.shooter.firstName']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='shot')|(pbp['event']=='shootout'),'event_primary_player_id'] = pbp['details.shooter.id']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout'),'event_primary_player_position'] = pbp['details.shooter.position']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked')|(pbp['event']=='shootout'),'event_primary_player_sweater_number'] = pbp['details.shooter.jerseyNumber']
    # faceoff, home win
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_name'] = pbp['details.homePlayer.firstName'] + ' ' +pbp['details.homePlayer.lastName']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_id'] = pbp['details.homePlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_position'] = pbp['details.homePlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_sweater_number'] = pbp['details.homePlayer.jerseyNumber']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_name'] = pbp['details.visitingPlayer.firstName'] + ' ' +pbp['details.visitingPlayer.lastName']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_id'] = pbp['details.visitingPlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_position'] = pbp['details.visitingPlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_sweater_number'] = pbp['details.visitingPlayer.jerseyNumber']
    # faceoff, away win
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_name'] = pbp['details.visitingPlayer.firstName'] + ' ' +pbp['details.visitingPlayer.lastName']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_id'] = pbp['details.visitingPlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_position'] = pbp['details.visitingPlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_sweater_number'] = pbp['details.visitingPlayer.jerseyNumber']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_name'] = pbp['details.homePlayer.firstName'] + ' ' +pbp['details.homePlayer.lastName']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_id'] = pbp['details.homePlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_position'] = pbp['details.homePlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_sweater_number'] = pbp['details.homePlayer.jerseyNumber']
    # goalie change, coming in
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_name'] = pbp['details.goalieComingIn.firstName'] + ' ' +pbp['details.goalieComingIn.lastName'] 
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_id'] = pbp['details.goalieComingIn.id']
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_position'] = pbp['details.goalieComingIn.position']
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_sweater_number'] = pbp['details.goalieComingIn.jerseyNumber']
    # goalie chhange, going out
    pbp.loc[pbp['event']=='goalie_change','event_secondary_player_name'] = pbp['details.goalieGoingOut.firstName'] + ' ' +pbp['details.goalieGoingOut.lastName'] 
    pbp.loc[pbp['event']=='goalie_change','event_secondary_player_id'] = pbp['details.goalieGoingOut.id']
    pbp.loc[pbp['event']=='goalie_change','event_secondary_player_position'] = pbp['details.goalieGoingOut.position']
    pbp.loc[pbp['event']=='goalie_change','event_secondary_player_sweater_number'] = pbp['details.goalieGoingOut.jerseyNumber']
    # penalty, taken by
    pbp.loc[pbp['event']=='penalty','event_primary_player_name'] = pbp['details.takenBy.firstName'] + ' ' +pbp['details.takenBy.lastName'] 
    pbp.loc[pbp['event']=='penalty','event_primary_player_id'] = pbp['details.takenBy.id']
    pbp.loc[pbp['event']=='penalty','event_primary_player_position'] = pbp['details.takenBy.position']
    pbp.loc[pbp['event']=='penalty','event_primary_player_sweater_number'] = pbp['details.takenBy.jerseyNumber']
    # penalty, served by
    pbp.loc[pbp['event']=='penalty','event_secondary_player_name'] = pbp['details.servedBy.firstName'] + ' ' +pbp['details.servedBy.lastName'] 
    pbp.loc[pbp['event']=='penalty','event_secondary_player_id'] = pbp['details.servedBy.id']
    pbp.loc[pbp['event']=='penalty','event_secondary_player_position'] = pbp['details.servedBy.position']
    pbp.loc[pbp['event']=='penalty','event_secondary_player_sweater_number'] = pbp['details.servedBy.jerseyNumber']
    # hit primary
    '''pbp.loc[pbp['event']=='hit','event_primary_player_name'] = pbp['details.player.firstName'] + ' ' +pbp['details.player.firstName']
    pbp.loc[pbp['event']=='hit','event_primary_player_id'] = pbp['details.player.id']
    pbp.loc[pbp['event']=='hit','event_primary_player_position'] = pbp['details.player.position']
    pbp.loc[pbp['event']=='hit','event_primary_player_sweater_number'] = pbp['details.player.jerseyNumber']'''
    '''
    # block secondary
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='shot'),'event_secondary_player_name'] = pbp['details.blocker.firstName'] + ' ' +pbp['details.shooter.firstName']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='shot'),'event_secondary_player_id'] = pbp['details.blocker.id']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot'),'event_secondary_player_position'] = pbp['details.blocker.position']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked'),'event_secondary_player_sweater_number'] = pbp['details.blocker.jerseyNumber']'''
    return pbp
a = scrape_game(21)
print(a[['event','event_primary_player_name','event_secondary_player_name']])
print(a['event'].value_counts())