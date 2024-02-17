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
import re
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
        pbp = pd.json_normalize(extract_json(pbp_text))
        pbp = add_misc_info(pbp,game_id)
        pbp = clean_pbp(pbp)
        print("Game {} finished.\n".format(game_id))
    return pbp

def extract_json(pbp_text):
    # Need to strip out the angular callbacks tag
    pattern = r'angular\.callbacks\._\d+\('
    # Use re.sub() to replace the matched pattern with an empty string at the beginning and ");" at the end
    json_str = re.sub(pattern, '', pbp_text).rstrip(');')
    #json_str = pbp_text.strip('angular.callbacks._8(').rstrip(');')
    # Parse the JSON string
    pbp_json = json.loads(json_str)
    return pbp_json

def add_misc_info(pbp,game_id):
    #For tons more of misc info not on the regualr pbp endpoint go to https://api-web.nhle.com/v1/gamecenter/2022030237/landing
    try:
        req = requests.get("https://lscluster.hockeytech.com/feed/index.php?feed=statviewfeed&view=gameSummary&game_id={}&key=694cfeed58c932ee&site_id=2&client_code=pwhl&lang=en&league_id=&callback=angular.callbacks._6".format(game_id))
        req.raise_for_status()
    except requests.exceptions.RequestException as req_exc:
        print(f"Gamecenter API request failed: {req_exc}")
    # Handle HTTP errors
    except requests.exceptions.HTTPError as http_err:
        print(f"Gamecenter API HTTP error occurred: {http_err}")
    # Handle value-related issues
    except ValueError as val_err:
        print(f"Gamecenter API Value error occured: {val_err}")
    else:
        misc_text = req.text 
        misc_json = extract_json(misc_text)
        home_team_id = misc_json['homeTeam']['info']['id']
        home_team_abbrev = misc_json['homeTeam']['info']['abbreviation']
        away_team_id = misc_json['visitingTeam']['info']['id']
        away_team_abbrev = misc_json['visitingTeam']['info']['abbreviation']
        game_id = misc_json['details']['id']
        date = misc_json['details']['GameDateISO8601'].split("T")[0]
        season_id = misc_json['details']['seasonId']
        pbp['home_team_id'] = home_team_id
        pbp['home_team'] = home_team_abbrev
        pbp['away_team_id'] = away_team_id
        pbp['away_team'] = away_team_abbrev
        pbp['game_id'] = game_id
        pbp['game_date']=date
        pbp['game_season_id'] = season_id
    return pbp

def clean_pbp(pbp):
    # clean players, to add in event players
    pbp = clean_players(pbp)
    # clean events

    return pbp


def clean_players(pbp):
    # make sure all columns are here
    pbp_df = check_columns(pbp)
    # shot, block, shootout, penaltyshot primary
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout')|(pbp['event']=='penaltyshot'),'event_primary_player_name'] = pbp['details.shooter.firstName'].apply(str) + ' ' +pbp['details.shooter.lastName'].apply(str)
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout')|(pbp['event']=='penaltyshot'),'event_primary_player_id'] = pbp['details.shooter.id']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout')|(pbp['event']=='penaltyshot'),'event_primary_player_position'] = pbp['details.shooter.position']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout')|(pbp['event']=='penaltyshot'),'event_primary_player_sweater_number'] = pbp['details.shooter.jerseyNumber']
    # faceoff, home win
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_name'] = pbp['details.homePlayer.firstName'].apply(str) + ' ' +pbp['details.homePlayer.lastName'].apply(str)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_id'] = pbp['details.homePlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_position'] = pbp['details.homePlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_sweater_number'] = pbp['details.homePlayer.jerseyNumber']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_name'] = pbp['details.visitingPlayer.firstName'].apply(str) + ' ' +pbp['details.visitingPlayer.lastName'].apply(str)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_id'] = pbp['details.visitingPlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_position'] = pbp['details.visitingPlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_sweater_number'] = pbp['details.visitingPlayer.jerseyNumber']
    # faceoff, away win
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_name'] = pbp['details.visitingPlayer.firstName'].apply(str) + ' ' +pbp['details.visitingPlayer.lastName'].apply(str)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_id'] = pbp['details.visitingPlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_position'] = pbp['details.visitingPlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_sweater_number'] = pbp['details.visitingPlayer.jerseyNumber']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_name'] = pbp['details.homePlayer.firstName'].apply(str) + ' ' +pbp['details.homePlayer.lastName'].apply(str)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_id'] = pbp['details.homePlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_position'] = pbp['details.homePlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_sweater_number'] = pbp['details.homePlayer.jerseyNumber']
    # goalie change, coming in
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_name'] = pbp['details.goalieComingIn.firstName'].apply(str) + ' ' +pbp['details.goalieComingIn.lastName'].apply(str)
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_id'] = pbp['details.goalieComingIn.id']
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_position'] = pbp['details.goalieComingIn.position']
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_sweater_number'] = pbp['details.goalieComingIn.jerseyNumber']
    # goalie chhange, going out, switching goalie
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)),'event_secondary_player_name'] = pbp['details.goalieGoingOut.firstName'].apply(str) + ' ' +pbp['details.goalieGoingOut.lastName'].apply(str)
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)),'event_secondary_player_id'] = pbp['details.goalieGoingOut.id']
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)),'event_secondary_player_position'] = pbp['details.goalieGoingOut.position']
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)),'event_secondary_player_sweater_number'] = pbp['details.goalieGoingOut.jerseyNumber']
    # goalie change, going out, pulling goalie
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)),'event_primary_player_name'] = pbp['details.goalieGoingOut.firstName'].apply(str) + ' ' +pbp['details.goalieGoingOut.lastName'].apply(str)
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)),'event_primary_player_id'] = pbp['details.goalieGoingOut.id']
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)),'event_primary_player_position'] = pbp['details.goalieGoingOut.position']
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)),'event_primary_player_sweater_number'] = pbp['details.goalieGoingOut.jerseyNumber']
    # penalty, taken by
    pbp.loc[pbp['event']=='penalty','event_primary_player_name'] = pbp['details.takenBy.firstName'].apply(str) + ' ' +pbp['details.takenBy.lastName'].apply(str)
    pbp.loc[pbp['event']=='penalty','event_primary_player_id'] = pbp['details.takenBy.id']
    pbp.loc[pbp['event']=='penalty','event_primary_player_position'] = pbp['details.takenBy.position']
    pbp.loc[pbp['event']=='penalty','event_primary_player_sweater_number'] = pbp['details.takenBy.jerseyNumber']
    # penalty, served by
    pbp.loc[pbp['event']=='penalty','event_secondary_player_name'] = pbp['details.servedBy.firstName'].apply(str) + ' ' +pbp['details.servedBy.lastName'].apply(str)
    pbp.loc[pbp['event']=='penalty','event_secondary_player_id'] = pbp['details.servedBy.id']
    pbp.loc[pbp['event']=='penalty','event_secondary_player_position'] = pbp['details.servedBy.position']
    pbp.loc[pbp['event']=='penalty','event_secondary_player_sweater_number'] = pbp['details.servedBy.jerseyNumber']
    # hit primary
    pbp.loc[pbp['event']=='hit','event_primary_player_name'] = pbp['details.player.firstName'].apply(str) + ' ' + pbp['details.player.firstName'].apply(str)
    pbp.loc[pbp['event']=='hit','event_primary_player_id'] = pbp['details.player.id']
    pbp.loc[pbp['event']=='hit','event_primary_player_position'] = pbp['details.player.position']
    pbp.loc[pbp['event']=='hit','event_primary_player_sweater_number'] = pbp['details.player.jerseyNumber']
    # block secondary
    pbp.loc[(pbp['event']=='blocked_shot'),'event_secondary_player_name'] = pbp['details.blocker.firstName'].apply(str) + ' ' +pbp['details.shooter.firstName'].apply(str)
    pbp.loc[(pbp['event']=='blocked_shot'),'event_secondary_player_id'] = pbp['details.blocker.id']
    pbp.loc[(pbp['event']=='blocked_shot'),'event_secondary_player_position'] = pbp['details.blocker.position']
    pbp.loc[(pbp['event']=='blocked_shot'),'event_secondary_player_sweater_number'] = pbp['details.blocker.jerseyNumber']
    # goal primary
    pbp.loc[pbp['event']=='goal','event_primary_player_name'] = pbp['details.scoredBy.firstName'].apply(str) + ' ' + pbp['details.scoredBy.lastName'].apply(str)
    pbp.loc[pbp['event']=='goal','event_primary_player_id'] = pbp['details.scoredBy.id']
    pbp.loc[pbp['event']=='goal','event_primary_player_position'] = pbp['details.scoredBy.position']
    pbp.loc[pbp['event']=='goal','event_primary_player_sweater_number'] = pbp['details.scoredBy.jerseyNumber']
    # need to extract assists
    pbp = extract_assists(pbp)
    # goal secondary
    pbp.loc[pbp['event']=='goal','event_secondary_player_name'] = pbp['assistor_1_firstName'].apply(str) + ' ' + pbp['assistor_1_lastName'].apply(str)
    pbp.loc[pbp['event']=='goal','event_secondary_player_id'] = pbp['assistor_1_id']
    pbp.loc[pbp['event']=='goal','event_secondary_player_position'] = pbp['assistor_1_position']
    pbp.loc[pbp['event']=='goal','event_secondary_player_sweater_number'] = pbp['assistor_1_jerseyNumber']
    # goal tertiary
    pbp.loc[pbp['event']=='goal','event_tertiary_player_name'] = pbp['assistor_2_firstName'].apply(str) + ' ' + pbp['assistor_2_lastName'].apply(str)
    pbp.loc[pbp['event']=='goal','event_tertiary_player_id'] = pbp['assistor_2_id']
    pbp.loc[pbp['event']=='goal','event_tertiary_player_position'] = pbp['assistor_2_position']
    pbp.loc[pbp['event']=='goal','event_tertiary_player_sweater_number'] = pbp['assistor_2_jerseyNumber']
    return pbp

def check_columns(pbp):
    cols = pbp.columns.tolist()
    needed = ['details.blocker.id','details.blocker.firstName','details.blocker.lastName','details.blocker.jerseyNumber','details.blocker.position','details.blocker.birthDate','details.blocker.playerImageURL',
             'details.player.id','details.player.firstName','details.player.lastName','details.player.jerseyNumber','details.player.position','details.player.birthDate','details.player.playerImageURL',
             'details.goalieGoingOut.firstName','details.goalieGoingOut.lastName','details.goalieGoingOut.id','details.goalieGoingOut.position','details.goalieGoingOut.jerseyNumber']
    difference = set(needed) - set(cols)
    # Convert the result back to a list
    result = list(difference)
    pbp[result] = np.nan
    return pbp

def extract_assists(pbp):
    # Apply the transformation
    player_info = pbp.apply(flatten_player_info, axis=1)
    pbp = pd.concat([pbp, player_info], axis=1)
    return pbp

def flatten_player_info(row):
    # Check if 'players' is a list
    if not isinstance(row['details.assists'], list):
        return pd.Series()
    # Container for the flattened data
    flattened = {}
    for i, player_dict in enumerate(row['details.assists']):
        # Ensure player_dict is a dictionary
        if not isinstance(player_dict, dict):
            continue
        for key, value in player_dict.items():
            # Construct new column name: e.g., player_0_id, player_1_firstName
            new_col_name = f'assistor_{i+1}_{key}'
            flattened[new_col_name] = value
    return pd.Series(flattened)


a = scrape_game(4)
#print(a[['event','event_primary_player_name','event_secondary_player_name','event_tertiary_player_name']])
print(a[a['event']=='goal'][['event','event_primary_player_name','event_secondary_player_name','event_tertiary_player_name','game_date']])