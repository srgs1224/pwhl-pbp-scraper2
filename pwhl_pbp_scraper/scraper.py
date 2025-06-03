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
    try:
        req = requests.get(
            "https://lscluster.hockeytech.com/feed/index.php?feed=statviewfeed&view=gameCenterPlayByPlay&game_id={}&key=694cfeed58c932ee&client_code=pwhl&lang=en&league_id=&callback=angular.callbacks._8".format(game_id)
        )
        req.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        print(f"Play-by-Play API HTTP error occurred: {http_err}")
        print("This game does not exist! Please enter a valid game id.")
        return None
    except requests.exceptions.RequestException as req_exc:
        print(f"Play-by-Play API request failed: {req_exc}")
    except ValueError as val_err:
        print(f"Play-by-Play API Value error occurred: {val_err}")
    else:
        pbp_text = req.text
        pbp = pd.json_normalize(extract_json(pbp_text))

        # 🔧 Fix period values before any type conversion
        if 'details.period.id' in pbp.columns:
            pbp['details.period.id'] = (
                pbp['details.period.id']
                .astype(str)
                .str.replace("'", "")
                .replace({
                    'OT1': 4,
                    'OT2': 5,
                    'OT3': 6,
                    'OT4': 7
                })
            )

        if len(pbp) == 0:
            print("This game does not exist! Please enter a valid game id.")
            return None
        else:
            pbp = add_header_trailer(pbp)
            pbp = add_misc_info(pbp, game_id)
            pbp = clean_pbp(pbp)
            print("Game {} finished.\n".format(game_id))
            return pbp  # ✅ Make sure this is indented correctly


def extract_json(pbp_text):
    # Need to strip out the angular callbacks tag
    pattern = r'angular\.callbacks\._\d+\('
    # Use re.sub() to replace the matched pattern with an empty string at the beginning and ");" at the end
    json_str = re.sub(pattern, '', pbp_text).rstrip(');')
    #json_str = pbp_text.strip('angular.callbacks._8(').rstrip(');')
    # Parse the JSON string
    pbp_json = json.loads(json_str)
    return pbp_json

def add_header_trailer(pbp):
    # create a DataFrame with an empty row
    empty_row = pd.DataFrame({col: np.nan for col in pbp.columns}, index=[0])
    # Concatenate the empty row DataFrame with the original DataFrame
    # Reset the index to maintain the order and drop the old index
    pbp['details.time'] = pbp['details.time'].fillna("5:00")
    pbp['details.period.id'] = pbp['details.period.id'].fillna("5")
    pbp = pd.concat([empty_row, pbp], ignore_index=True)
    pbp.iloc[0, pbp.columns.get_loc('details.time')] = "0:00"
    pbp.iloc[0, pbp.columns.get_loc('details.period.id')] = "1"
    pbp.iloc[0, pbp.columns.get_loc('event')] = "start_of_game"
    max_period = pbp['details.period.id'].max()
    # account for shootout
    if "shootout" in pbp['event'].value_counts().keys().tolist():
        max_period = 5
    pbp = pd.concat([pbp, empty_row], ignore_index=True)
    pbp.loc[pbp.index[-1], 'details.period.id'] = max_period
    pbp['shifted_time'] = pbp['details.time'].shift(1)
    pbp.loc[pbp.index[-1], 'event'] = "end_of_game"
    pbp.loc[pbp['event']=='end_of_game','details.time'] = pbp['shifted_time']
    return pbp

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
    # make sure all columns are here
    pbp = check_columns(pbp)
    # adding this function in to catch any weird data issues
    pbp = check_values(pbp)
    # clean players, to add in event players
    pbp = clean_players(pbp)
    # clean events
    pbp = clean_events(pbp)
    # clean teams
    pbp = clean_teams(pbp)
    # clean time
    pbp = clean_time(pbp)
    # add goalies
    pbp = add_goalies(pbp)
    # add score
    pbp = add_score(pbp)
    # build description
    pbp = build_desc(pbp)
    # format
    pbp = format_pbp(pbp)
    return pbp

def check_columns(pbp):
    cols = pbp.columns.tolist()
    needed = ['details.blocker.id','details.blocker.firstName','details.blocker.lastName','details.blocker.jerseyNumber','details.blocker.position','details.blocker.birthDate','details.blocker.playerImageURL',
             'details.player.id','details.player.firstName','details.player.lastName','details.player.jerseyNumber','details.player.position','details.player.birthDate','details.player.playerImageURL',
             'details.goalieGoingOut.firstName','details.goalieGoingOut.lastName','details.goalieGoingOut.id','details.goalieGoingOut.position','details.goalieGoingOut.jerseyNumber','details.teamId','details.shooter_team.id','details.shooterTeam.id','details.isGameWinningGoal']
    difference = set(needed) - set(cols)
    # Convert the result back to a list
    result = list(difference)
    pbp[result] = np.nan
    return pbp

def clean_players(pbp):
    # shot, block, shootout, penaltyshot primary
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout')|(pbp['event']=='penaltyshot'),'event_primary_player_name'] = pbp['details.shooter.firstName'].apply(str) + ' ' +pbp['details.shooter.lastName'].apply(str)
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout')|(pbp['event']=='penaltyshot'),'event_primary_player_id'] = pbp['details.shooter.id']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout')|(pbp['event']=='penaltyshot'),'event_primary_player_position'] = pbp['details.shooter.position']
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout')|(pbp['event']=='penaltyshot'),'event_primary_player_sweater_number'] = pbp['details.shooter.jerseyNumber'].fillna("0").astype(int)
    # faceoff, home win
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_name'] = pbp['details.homePlayer.firstName'].apply(str) + ' ' +pbp['details.homePlayer.lastName'].apply(str)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_id'] = pbp['details.homePlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_position'] = pbp['details.homePlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_primary_player_sweater_number'] = pbp['details.homePlayer.jerseyNumber'].fillna("0").astype(int)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_name'] = pbp['details.visitingPlayer.firstName'].apply(str) + ' ' +pbp['details.visitingPlayer.lastName'].apply(str)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_id'] = pbp['details.visitingPlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_position'] = pbp['details.visitingPlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='1')),'event_secondary_player_sweater_number'] = pbp['details.visitingPlayer.jerseyNumber'].fillna("0").astype(int)
    # faceoff, away win
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_name'] = pbp['details.visitingPlayer.firstName'].apply(str) + ' ' +pbp['details.visitingPlayer.lastName'].apply(str)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_id'] = pbp['details.visitingPlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_position'] = pbp['details.visitingPlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_primary_player_sweater_number'] = pbp['details.visitingPlayer.jerseyNumber'].fillna("0").astype(int)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_name'] = pbp['details.homePlayer.firstName'].apply(str) + ' ' +pbp['details.homePlayer.lastName'].apply(str)
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_id'] = pbp['details.homePlayer.id']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_position'] = pbp['details.homePlayer.position']
    pbp.loc[((pbp['event']=='faceoff')&(pbp['details.homeWin']=='0')),'event_secondary_player_sweater_number'] = pbp['details.homePlayer.jerseyNumber'].fillna("0").astype(int)
    # goalie change, coming in
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_name'] = pbp['details.goalieComingIn.firstName'].apply(str) + ' ' +pbp['details.goalieComingIn.lastName'].apply(str)
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_id'] = pbp['details.goalieComingIn.id']
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_position'] = pbp['details.goalieComingIn.position']
    pbp.loc[pbp['event']=='goalie_change','event_primary_player_sweater_number'] = pbp['details.goalieComingIn.jerseyNumber'].fillna("0").astype(int)
    # goalie chhange, going out, switching goalie
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)),'event_secondary_player_name'] = pbp['details.goalieGoingOut.firstName'].apply(str) + ' ' +pbp['details.goalieGoingOut.lastName'].apply(str)
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)),'event_secondary_player_id'] = pbp['details.goalieGoingOut.id']
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)),'event_secondary_player_position'] = pbp['details.goalieGoingOut.position']
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)),'event_secondary_player_sweater_number'] = pbp['details.goalieGoingOut.jerseyNumber'].fillna("0").astype(int)
    # goalie change, going out, pulling goalie
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)),'event_primary_player_name'] = pbp['details.goalieGoingOut.firstName'].apply(str) + ' ' +pbp['details.goalieGoingOut.lastName'].apply(str)
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)),'event_primary_player_id'] = pbp['details.goalieGoingOut.id']
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)),'event_primary_player_position'] = pbp['details.goalieGoingOut.position']
    pbp.loc[((pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)),'event_primary_player_sweater_number'] = pbp['details.goalieGoingOut.jerseyNumber'].fillna("0").astype(int)
    # penalty, taken by
    pbp.loc[pbp['event']=='penalty','event_primary_player_name'] = pbp['details.takenBy.firstName'].apply(str) + ' ' +pbp['details.takenBy.lastName'].apply(str)
    pbp.loc[pbp['event']=='penalty','event_primary_player_id'] = pbp['details.takenBy.id']
    pbp.loc[pbp['event']=='penalty','event_primary_player_position'] = pbp['details.takenBy.position']
    pbp.loc[pbp['event']=='penalty','event_primary_player_sweater_number'] = pbp['details.takenBy.jerseyNumber'].fillna("0").astype(int)
    # hit primary
    pbp.loc[pbp['event']=='hit','event_primary_player_name'] = pbp['details.player.firstName'].apply(str) + ' ' + pbp['details.player.lastName'].apply(str)
    pbp.loc[pbp['event']=='hit','event_primary_player_id'] = pbp['details.player.id']
    pbp.loc[pbp['event']=='hit','event_primary_player_position'] = pbp['details.player.position']
    pbp.loc[pbp['event']=='hit','event_primary_player_sweater_number'] = pbp['details.player.jerseyNumber'].fillna("0").astype(int)
    # block secondary
    pbp.loc[(pbp['event']=='blocked_shot'),'event_secondary_player_name'] = pbp['details.blocker.firstName'].apply(str) + ' ' +pbp['details.shooter.firstName'].apply(str)
    pbp.loc[(pbp['event']=='blocked_shot'),'event_secondary_player_id'] = pbp['details.blocker.id']
    pbp.loc[(pbp['event']=='blocked_shot'),'event_secondary_player_position'] = pbp['details.blocker.position']
    pbp.loc[(pbp['event']=='blocked_shot'),'event_secondary_player_sweater_number'] = pbp['details.blocker.jerseyNumber'].fillna("0").astype(int)
    # goal primary
    pbp.loc[pbp['event']=='goal','event_primary_player_name'] = pbp['details.scoredBy.firstName'].apply(str) + ' ' + pbp['details.scoredBy.lastName'].apply(str)
    pbp.loc[pbp['event']=='goal','event_primary_player_id'] = pbp['details.scoredBy.id']
    pbp.loc[pbp['event']=='goal','event_primary_player_position'] = pbp['details.scoredBy.position']
    pbp.loc[pbp['event']=='goal','event_primary_player_sweater_number'] = pbp['details.scoredBy.jerseyNumber'].fillna("0").astype(int)
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
    pbp.loc[pbp['event']=='goal','event_tertiary_player_sweater_number'] = pbp['assistor_2_jerseyNumber'].fillna("0").astype(int)
    # goalie against
    pbp['goalie_against_name'] = pbp['details.goalie.firstName'] + " " + pbp['details.goalie.lastName'] 
    pbp['goalie_against_id'] = pbp['details.goalie.id']
    pbp['goalie_against_sweater_number'] = pbp['details.goalie.jerseyNumber']
    # erase weird nulls
    pbp.loc[pbp['event_secondary_player_name']=="nan nan", 'event_secondary_player_name'] = np.nan
    pbp.loc[pbp['event_tertiary_player_name']=="nan nan", 'event_tertiary_player_name'] = np.nan
    return pbp

def check_values(pbp):
    # check to make sure OT1 isn't in period ID column. saw this in game 36
    pbp.loc[pbp['details.period.id']=="OT1","details.period.id"] = "4"
    return pbp

def extract_assists(pbp):
    # apply the transformation
    player_info = pbp.apply(flatten_player_info, axis=1)
    pbp = pd.concat([pbp, player_info], axis=1)
    return pbp

def flatten_player_info(row):
    # check if 'players' is a list
    if not isinstance(row['details.assists'], list):
        return pd.Series()
    # container for the flattened data
    flattened = {}
    for i, player_dict in enumerate(row['details.assists']):
        # ensure player_dict is a dictionary
        if not isinstance(player_dict, dict):
            continue
        for key, value in player_dict.items():
            # construct new column name: e.g., player_0_id, player_1_firstName
            new_col_name = f'assistor_{i+1}_{key}'
            flattened[new_col_name] = value
    return pd.Series(flattened)

def clean_events(pbp):
    # change shootout event to shootout-shot or shootout-goal
    pbp.loc[(pbp['event']=='shootout')&(pbp['details.isGoal']==True),'event'] = "shootout_goal"
    pbp.loc[(pbp['event']=='shootout')&(pbp['details.isGoal']==False),'event'] = "shootout_shot"
    pbp.loc[(pbp['event']=='penaltyshot')&(pbp['details.isGoal']==True),'event'] = "peanlty_shot_goal"
    pbp.loc[(pbp['event']=='penaltyshot')&(pbp['details.isGoal']==False),'event'] = "penalty_shot_shot"
    pbp.loc[(pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)&(pbp['details.goalieGoingOut.id'].isna()==0),'event'] = 'goalie_sub'
    pbp.loc[(pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==1)&(pbp['details.goalieGoingOut.id'].isna()==0),'event'] = 'goalie_pull'
    pbp.loc[(pbp['event']=='goalie_change')&(pbp['details.goalieComingIn.id'].isna()==0)&(pbp['details.goalieGoingOut.id'].isna()==1),'event'] = 'goalie_entrance'
    # for some reason the data counts goals in 2 rows. first as a shot, next as a goal. we're gonna need to fix that
    pbp.loc[pbp['event']=="goal",'details.isGoal'] = True
    pbp.loc[(pbp['event']=="shot")&(pbp['details.isGoal']==True),'delete_this_row'] = 1
    pbp.loc[(pbp['event']=="shot")&(pbp['details.isGoal']==True),'details.isGoal'] = False
    pbp['shifted_shot_type'] = pbp['details.shotType'].shift(1)
    pbp['shifted_shot_quality'] = pbp['details.shotQuality'].shift(1)
    pbp.loc[pbp['event']=="goal",'details.shotType'] = pbp['shifted_shot_type']
    pbp.loc[pbp['event']=="goal",'details.shotQuality'] = pbp['shifted_shot_quality']
    pbp = pbp[pbp['delete_this_row']!=1]
    return pbp

def clean_teams(pbp):
    pbp = pbp.astype(object)
    # goal
    pbp.loc[(pbp['event']=='goal'),'event_team_id'] =  pbp['details.team.id'].fillna("0").astype(int)
    # shot, blocked shot
    pbp.loc[(pbp['event']=='shot')|(pbp['event']=='blocked_shot')|(pbp['event']=='shootout_shot')|(pbp['event']=='shootout_goal'),'event_team_id'] =  pbp['details.shooterTeamId'].fillna("0").astype(int)
    # faceoff
    pbp.loc[(pbp['event']=='faceoff')&(pbp['details.homeWin']=="1"),'event_team_id'] =  pbp['home_team_id'].fillna("0").astype(int)
    pbp.loc[(pbp['event']=='faceoff')&(pbp['details.homeWin']=="0"),'event_team_id'] =  pbp['away_team_id'].fillna("0").astype(int)
    # hit
    pbp.loc[(pbp['event']=='hit'),'event_team_id'] =  pbp['details.teamId'].fillna("0").astype(int)
    # penalty
    pbp.loc[(pbp['event']=='penalty'),'event_team_id'] =  pbp['details.againstTeam.id'].fillna("0").astype(int)
    # penalty shot
    pbp.loc[(pbp['event']=='penalty_shot_shot')|(pbp['event']=='penalty_shot_goal'),'event_team_id'] =  pbp['details.shooter_team.id'].fillna("0").astype(int)
    # goalie change
    pbp.loc[(pbp['event']=='goalie_entrance'),'event_team_id'] =  pbp['details.team_id'].fillna("0").astype(int)
    pbp.loc[(pbp['event']=='goalie_pull'),'event_team_id'] =  pbp['details.team_id'].fillna("0").astype(int)
    pbp.loc[(pbp['event']=='goalie_sub'),'event_team_id'] =  pbp['details.team_id'].fillna("0").astype(int)
    # shootout
    pbp.loc[(pbp['event']=='shootout_shot')|(pbp['event']=='shootout_goal'),'event_team_id'] = pbp['details.shooterTeam.id'].fillna("0").astype(int)
    pbp['event_team_id'] = pbp['event_team_id'].fillna("0").astype(int)
    # map team abbrev
    home_id = pbp.iloc[0]['home_team_id']
    home_name = pbp.iloc[0]['home_team']
    away_id = pbp.iloc[0]['away_team_id']
    away_name = pbp.iloc[0]['away_team']
    team_dict = {}
    team_dict[home_id] = home_name
    team_dict[away_id] = away_name
    pbp['event_team'] = pbp['event_team_id'].map(team_dict)
    return pbp

def build_desc(pbp):
    pbp['description'] = ""
    # goal
    pbp.loc[(pbp['event']=='goal')&(pbp['event_secondary_player_id'].isna()==True),'description'] = pbp['event_team']+ " goal scored by "+ pbp['event_primary_player_name'] + ", unassisted"
    pbp.loc[(pbp['event']=='goal')&(pbp['event_secondary_player_id'].isna()==False)&(pbp['event_tertiary_player_id'].isna()==True),'description'] = pbp['event_team']+ " goal scored by "+ pbp['event_primary_player_name'] + ", assisted by " + pbp['event_secondary_player_name']
    pbp.loc[(pbp['event']=='goal')&(pbp['event_secondary_player_id'].isna()==False)&(pbp['event_tertiary_player_id'].isna()==False),'description'] = pbp['event_team']+ " goal scored by "+ pbp['event_primary_player_name'] + ", assisted by " + pbp['event_secondary_player_name'] + " and " +  pbp['event_tertiary_player_name']
    # shot
    pbp.loc[(pbp['event']=='shot'),'description'] = pbp['event_team']+ " shot by "+ pbp['event_primary_player_name']
    # blocked shot
    pbp.loc[(pbp['event']=='blocked_shot'),'description'] = pbp['event_team']+ " blocked shot, shot by "+ pbp['event_primary_player_name'] + ', blocked by ' + " " + pbp['event_secondary_player_name']
    # faceoff
    pbp.loc[(pbp['event']=='faceoff')&(pbp['details.homeWin']=='0'),'description'] = pbp['event_team']+ " faceoff won by "+ pbp['event_primary_player_name'] + ', lost by '+ pbp['home_team'] + ' ' +pbp['event_secondary_player_name']
    pbp.loc[(pbp['event']=='faceoff')&(pbp['details.homeWin']=='1'),'description'] = pbp['event_team']+ " faceoff won by "+ pbp['event_primary_player_name'] + ', lost by '+ pbp['away_team'] + ' ' +pbp['event_secondary_player_name']
    # hit
    pbp.loc[(pbp['event']=='hit'),'description'] = pbp['event_team']+ " hit thrown by "+ pbp['event_primary_player_name']
    # goalie change
    pbp.loc[(pbp['event']=='goalie_sub'),'description'] = pbp['event_team']+ " goalie substitution, "+ pbp['event_primary_player_name'] + " entering the game for " + pbp['event_secondary_player_name']
    pbp.loc[(pbp['event']=='goalie_pull'),'description'] = pbp['event_team']+ " goalie pull,  "+ pbp['event_primary_player_name'] + " being pulled from the game"
    pbp.loc[(pbp['event']=='goalie_entrance'),'description'] = pbp['event_team']+ " goalie entrance, " + pbp['event_primary_player_name'] + " entering the game"
    # penalty
    pbp.loc[(pbp['event']=='penalty'),'description'] = pbp['event_team']+ " penalty, taken by " + pbp['event_primary_player_name'] + ", " + pbp['details.minutes'].fillna("0").astype(float).astype(int).astype(str)+ " minutes for " + pbp['details.description']
    # shootout shot
    pbp.loc[pbp['event']=="shootout_shot",'description'] = pbp['event_team'] + " shootout shot by " + pbp['event_primary_player_name']
    # shootout goal
    pbp.loc[(pbp['event']=="shootout_goal")&(pbp['details.isGameWinningGoal']==False),'description'] = pbp['event_team'] + " shootout goal by " + pbp['event_primary_player_name']
    # shootout goal
    pbp.loc[(pbp['event']=="shootout_goal")&(pbp['details.isGameWinningGoal']==True),'description'] = pbp['event_team'] + " game winning shootout goal by " + pbp['event_primary_player_name']
    # game start
    pbp.loc[pbp['event']=='start_of_game','description'] = pbp['away_team'] + " @ " + pbp['home_team'] + ", start of game"
    # game end
    pbp.loc[pbp['event']=='end_of_game','description'] = pbp['away_team'] + " @ " + pbp['home_team'] + ", end of game. Final score: " + pbp['away_team'] + " " + pbp['away_score'].astype(str) + " - " + pbp['home_score'].astype(str) + " " + pbp['home_team']
    # penalty shot shot
    pbp.loc[pbp['event']=='penalty_shot_shot','description'] = pbp['event_team'] + " penalty shot attempt by " + pbp['event_primary_player_name']
    # penalty shot goal
    pbp.loc[pbp['event']=='penalty_shot_goal','description'] = pbp['event_team'] + " penalty shot goal scored by " + pbp['event_primary_player_name']
    return pbp

def clean_time(pbp):
    # function to convert time elapsed to seconds
    pbp['details.time'] = pbp['details.time'].fillna("5:00")
    pbp['details.period.id'] = pbp['details.period.id'].fillna("5")
    convert_to_seconds_vectorized(pbp, ['details.time'])
    # change from period seconds to game seconds
    period_condition = pbp['details.period.id'] != 1
    for time_col in ['details.time_seconds']:
        pbp[time_col] = np.where(period_condition, pbp[time_col] + 1200 * (pbp['details.period.id'].astype(int) - 1), pbp[time_col])
    # account for shootouts that will occur at min 65 of the game
    pbp.loc[(pbp['details.period.id']=="5")&((pbp['event']=='shootout_shot')|(pbp['event']=='shootout_goal')|(pbp['event']=='shootout')),'details.time_seconds'] = 3900
    pbp['game_minutes_elapsed'] = pbp['details.time_seconds']/60

    return pbp

def convert_to_seconds_vectorized(shifts, col_names):
    '''
    convert_to_seconds_vectorized - Function to convert the time columns to seconds
    parameters - shifts - A data frame of extracted shift data, col_names - column names to convert
    '''
    for col in col_names:
        time_parts = shifts[col].str.split(':', expand=True).astype(int)
        shifts[f'{col}_seconds'] = time_parts[0] * 60 + time_parts[1]

# add goalies
def add_goalies(pbp):
    # Initialize dictionaries to keep track of the current goalies for home and away teams
    current_home_goalies = {}  # Format: {game_id: goalie_name}
    current_away_goalies = {}  # Format: {game_id: goalie_name}
    
    # Initialize lists to hold the current goalie for each event for home and away teams
    current_home_goalie_per_event = []
    current_away_goalie_per_event = []

    # Iterate through each row in the DataFrame
    for index, row in pbp.iterrows():
        if (row['event'] == 'goalie_entrance')|(row['event'] == 'goalie_sub'):
            # Update the current goalie for the corresponding team
            if row['event_team'] == row['home_team']:
                current_home_goalies[row['game_id']] = row['event_primary_player_name']
            else:
                current_away_goalies[row['game_id']] = row['event_primary_player_name']
        elif row['event'] == 'goalie_pull':
            if row['event_team'] == row['home_team']:
                current_home_goalies[row['game_id']] = np.nan
            else:
                current_away_goalies[row['game_id']] = np.nan
        # Append the current goalie for the home and away teams to their respective lists
        current_home_goalie_per_event.append(current_home_goalies.get(row['game_id'], None))
        current_away_goalie_per_event.append(current_away_goalies.get(row['game_id'], None))

    # Add the current goalie information as new columns to the DataFrame
    pbp['current_home_goalie'] = current_home_goalie_per_event
    pbp['current_away_goalie'] = current_away_goalie_per_event
    return pbp

def add_score(pbp):
    pbp['is_goal'] = pbp['details.isGoal'].map({True:1,False:0})
    pbp['is_goal'] = pbp['is_goal'].fillna("0").astype(int)
    pbp['isHomeGoal']=0
    pbp['isAwayGoal']=0
    pbp.loc[((pbp['is_goal']==1)&(pbp['event_team']==pbp['home_team'])&(pbp['event']!='shootout_goal')),"isHomeGoal"] = 1
    pbp.loc[((pbp['is_goal']==1)&(pbp['event_team']==pbp['away_team'])&(pbp['event']!='shootout_goal')),"isAwayGoal"] = 1
    #pbp.loc[((pbp['is_goal']==1)&(pbp['event_team']==pbp['home_team'])&(pbp['event']=='shootout_goal')&(pbp['details.isGameWinningGoal']==True)),"isHomeGoal"] = 1
    #pbp.loc[((pbp['is_goal']==1)&(pbp['event_team']==pbp['away_team'])&(pbp['event']=='shootout_goal')&(pbp['details.isGameWinningGoal']==True)),"isAwayGoal"] = 1
    pbp['away_score'] = pbp['isAwayGoal'].cumsum()
    pbp['home_score'] = pbp['isHomeGoal'].cumsum()
    pbp.loc[((pbp['is_goal']==1)&(pbp['event_team']==pbp['home_team'])&(pbp['event']!='shootout_goal')),'home_score'] = pbp['home_score']-1
    pbp.loc[((pbp['is_goal']==1)&(pbp['event_team']==pbp['away_team'])&(pbp['event']!='shootout_goal')),'away_score'] = pbp['away_score']-1
    # for shootouts
    if "shootout_shot" in pbp['event'].value_counts().keys().tolist():
        shootout_goals = pbp[pbp['event']=="shootout_goal"]
        shootout_goals.groupby("event_team").count().reset_index()
        home_team = pbp.iloc[0]['home_team']
        away_team = pbp.iloc[0]['away_team']
        more_goals = shootout_goals['event_team'].value_counts().sort_values().keys()[0]
        if more_goals == home_team:
            pbp.loc[pbp['event']=="end_of_game",'home_score'] = pbp['home_score'].max()+1
        else:
            pbp.loc[pbp['event']=="end_of_game",'away_score'] = pbp['away_score'].max()+1
    return pbp

def format_pbp(pbp):
    # final cleanup of the df
    # rename some cols
    # period
    pbp = pbp.rename(columns={"details.period.id":"period","details.xLocation":"xC","details.yLocation":"yC","details.shotType":"shot_type","details.shotQuality":"shot_quality",
                              "details.properties.isPowerPlay":"is_power_play","details.properties.isShortHanded":"is_short_handed","details.properties.isEmptyNet":"is_on_empty_net",
                              "details.properties.isPenaltyShot":"is_penalty_shot","details.properties.isGameWinningGoal":"is_game_winning_goal","details.time_seconds":"game_seconds_elapsed"})
    pbp = pbp[['game_id','game_date','home_team','home_team_id','away_team','away_team_id','period','game_seconds_elapsed','game_minutes_elapsed','event','event_team','event_primary_player_name','event_primary_player_id','event_primary_player_position','event_primary_player_sweater_number'
               ,'event_secondary_player_name','event_secondary_player_id','event_secondary_player_position','event_secondary_player_sweater_number'
             ,'event_tertiary_player_name','event_tertiary_player_id','event_tertiary_player_position','event_tertiary_player_sweater_number','description','shot_type','shot_quality','is_power_play','is_short_handed','is_on_empty_net','is_penalty_shot','is_game_winning_goal','xC','yC','away_score','home_score','current_home_goalie','current_away_goalie']]
    return pbp
