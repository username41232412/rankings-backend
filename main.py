# features to implement:
# 1. able to take back updates. If a trusted host makes a mistake by
# recording games he shouldn't, or trusted host gone rogue, we should
# be able to reverse the bad updates
# 2. do not accept multiple requests at the same time that all have 
# valid keys, but do log it. This should never happen unless someone
# has modified their lua (because lua should be built to only
# record if you are the host of the current server), so when it happens 
# it may be a sign that someone with a valid key going rogue.
# 3. be able to issue commands like to:
#      add key to database
#      revoke key from database
#      take back updates
#      etc. at least pave a way to run commands related to the project
#      and adding commands should be easy
# commands should be sent to route "/commands". A new function with 
# @app.route('/post', methods=['POST'])
# These should also be validated with keys checked from a table of
# admin keys separate from the table of trusted-host keys.

# scenarios to remember:
# 1. one team has no members - fixed
# 2. key is wrong - fixed
# 3. make lua only able to record when you are host
# 4. make lua give warning window to the recorder that recording is active 
# when entering server, reminding to disable the content package if 
# not intending to record, and also buttons to ignore or temporarily disable 
# for current session.



from flask import Flask, request
import threading
import math
import time
import requests

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from google.api_core.exceptions import BadRequest

google_client = bigquery.Client()
DEFAULT_ELO = 500


app = Flask(__name__)

# Function to calculate the expected outcome
# rating_a = team a's rating
# rating_b = team b's rating
# outputs probability of win for team a
def calculate_expected_outcome(rating_a, rating_b):
    return 1 / (1 + 10 ** ((rating_b - rating_a) / 400))

# Function to calculate a teams rating
# ratings = a list of numbers representing the ratings of each player in that team
def calculate_team_rating(ratings):
	sum = 0
	for rating in ratings:
		sum = sum + 10 ** ((rating - 500) / 400)
	return int(round(400 * math.log10(sum) + 500))
	
# Function to update rating
def update_rating(rating, expected, actual, k=32):
    return int(round(rating + k * (actual - expected)))

# Function to search through database for steamID's rating
def get_rating(steamID):
    QUERY = """
        SELECT elo
        FROM `Main.rankings`
        WHERE steamid = @steamid
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("steamid", "STRING", steamID)
        ]
    )

    try:
        query_job = google_client.query(QUERY, job_config=job_config)
        rows = query_job.result()

        if query_job.state == 'DONE':
            for row in rows:
                print(f"Elo for steamid {steamID} is {row.elo}")
                return row.elo
            print(f"Elo not found for steamid {steamID}")
            return None
        else:
            print(f"Query job did not complete successfully. Job state: {query_job.state}")
            return None  # Operation was not successful
    except GoogleAPIError as e:
        print(f"An error occurred: {e}")
        return None  # Operation failed

# Function to search through database for matching key
def is_valid_key(key):
    QUERY = """
        SELECT key, `owner-steam-id`, `owner-name`
        FROM `Main.keys`
        WHERE key = @key_value
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("key_value", "STRING", key)
        ]
    )

    try:
        query_job = google_client.query(QUERY, job_config=job_config)
        rows = query_job.result()  # Waits for the query to finish

        if query_job.state == 'DONE':
            for row in rows:
                print(f"Key: {row.key}, Owner Steam ID: {row['owner-steam-id']}, Owner Name: {row['owner-name']}")
                return True
            return False
        else:
            print(f"Query job did not complete successfully. Job state: {query_job.state}")
            return False  # Operation was not successful
    except GoogleAPIError as e:
        print(f"An error occurred: {e}")
        return False  # Operation failed

# Function to update database with real rating
def change_rating(steamid, name, new_rating, nationality = None):
    timestamp = int(time.time())
    rows_to_insert = [{"steamid": steamid, "name": name, "elo": new_rating, "timestamp": timestamp, "nationality": nationality}]
    table_id = "Main.rankings"  # Update with your actual table ID

    try:
        errors = google_client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            print("Query job did not complete successfully. Errors:")
            for error in errors:
                print(error)
        else:
            print(f"Elo updated for steamid {steamid} to {new_rating}")
            return True  # Operation was successful

    except GoogleAPIError as e:
        print(f"An error occurred: {e}")
        return False  # Operation failed


def get_rows_for_clients(steamIDs):
    # Construct the query
    QUERY = """
        SELECT r.*
        FROM Main.rankings r
        JOIN (
            SELECT steamid, MAX(timestamp) AS max_timestamp
            FROM Main.rankings
            WHERE steamid IN UNNEST(@steamids)
            GROUP BY steamid
        ) max_timestamps
        ON r.steamid = max_timestamps.steamid
        AND r.timestamp = max_timestamps.max_timestamp;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("steamids", "STRING", steamIDs)
        ]
    )

    try:
        query_job = google_client.query(QUERY, job_config=job_config)
        rows = query_job.result()  # Waits for the query to finish

        results = {}
        found_steamids = set()

        for row in rows:
            results[row.steamid] = {'name': row.name, 'steamid': row.steamid, 'elo': row.elo, 'nationality': row.nationality}
            found_steamids.add(row.steamid)

        not_found_steamids = set(steamIDs) - found_steamids

        if not results:
            print("No rows found for the provided steamIDs.")

        return results, list(not_found_steamids)

    except GoogleAPIError as e:
        print(f"An error occurred: {e}")
        return None  # Operation failed

def make_rows_for_clients(not_found_steamids, names):
    if len(not_found_steamids) != len(names):
        raise ValueError("The length of not_found_steamids and names must be the same.")

    timestamp = int(time.time())
    rows_to_insert = [
        {"steamid": steamid, "name": name, "elo": DEFAULT_ELO, "timestamp": timestamp}  # Default elo value
        for steamid, name in zip(not_found_steamids, names)
    ]

    table_id = "Main.rankings"  # Update with your actual table ID
    try:
        errors = google_client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            print("Errors occurred while inserting rows:")
            for error in errors:
                print(error)
        else:
            print("Rows successfully inserted.")

    except GoogleAPIError as e:
        print(f"An error occurred: {e}")

def change_names(to_change):
    for steamid, name, elo in to_change:
        change_name(steamid, name, elo)

def change_name(steamid, name, elo):
    timestamp = int(time.time())
    rows_to_insert = [{"steamid": steamid, "name": name, "elo": elo, "timestamp": timestamp}]
    table_id = "Main.rankings"

    try:
        errors = google_client.insert_rows_json(table_id, rows_to_insert)

        if errors:
            print("Errors occurred while inserting rows:")
            for error in errors:
                print(error)
            return False  # Operation was not successful
        else:
            print(f"Name for steamID {steamid} has been changed to {name}")
            return True  # Operation was successful
    except GoogleAPIError as e:
        print(f"An error occurred: {e}")
        return False  # Operation failed

def delete_old(steamids):
    for steamid in steamids:
        # Query to select all rows with the specified steamid
        query = f"""
            SELECT *
            FROM Main.rankings
            WHERE steamid = '{steamid}'
            ORDER BY timestamp DESC
        """
        # Execute the query
        rows_to_delete = google_client.query(query).result()

        # Keep track of the highest timestamp row to retain
        highest_timestamp_row = None

        # Iterate over the rows for the given steamid
        for row in rows_to_delete:
            #print("")
            #print(row)
            # Retain the row with the highest timestamp
            if highest_timestamp_row is None:
                highest_timestamp_row = row
            else:
                # Delete the row if it's not the highest timestamp
                try:
                    QUERY = f"""
                    DELETE FROM `Main.rankings`
                    WHERE timestamp = @timestamp
                    AND steamid = @steamid
                    AND elo = @elo
                    AND name = @name
                    """

                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("steamid", "STRING", row.steamid),
                            bigquery.ScalarQueryParameter("name", "STRING", row.name),
                            bigquery.ScalarQueryParameter("elo", "INT64", row.elo),
                            bigquery.ScalarQueryParameter("timestamp", "INT64", row.timestamp)
                        ]
                    )

                    query_job = google_client.query(QUERY, job_config=job_config)
                    #query_job.result()  # Waits for the query to finish
                    print(".",end='',flush=True)

                # this never happens anyways since I disabled "query_job.result()"
                except BadRequest as e:
                    print(",",end='',flush=True)
                    #print(f"Error deleting row: {steamid} - {e}")
                except Exception as e:
                    print(f"Error deleting row: {steamid} - {type(e)}")

        # Print a message indicating the rows deleted for the steamid
        if highest_timestamp_row is not None:
            print("|",end='',flush=True)
    print('')


def delete_old_withThreads(steamids):
    print("Starting new thread to delete old entries.")
    threading.Thread(target=delete_old, kwargs={"steamids": steamids}).start()

def alert_bot(match_data=None):
    # Update this URL to point to your Discord bot's App Engine service
    url = 'https://discord-bot-ranks-dot-bplrankings.appspot.com/update-rankings'
    print("Sending alert to discord bot...")

    # Prepare payload with match data if available
    payload = {}
    if match_data:
        payload = match_data

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("Discord bot successfully notified")
        else:
            print(f"Discord bot notification failed with status code: {response.status_code}")
    except Exception as e:
        print(f"Alert to bot failed: {e}")

@app.route('/')
def hello_world():
    return 'Send match data to this url\'s \"/post\" route'

@app.route('/post', methods=['POST'])
def receive_post():
    if request.method == 'POST':
        data = request.get_json()  # Parsing JSON data from the request
        print("")
        print("REQUEST RECEIVED")
        print("----------------")
        print("Request Data: ", data)

        key = data['key']
        if is_valid_key(key) == False:
            print("Invalid key")
            return "Your lua's key is invalid."

        team1_ratings = []
        team2_ratings = []
        rating_changes = {}

        # For match data to send to discord bot
        match_data = {
            "timestamp": int(time.time()),
            "winning_team": data.get('winning_team'),
            "teams": {
                "1": [],
                "2": []
            },
            "team_ratings": {},
            "expected_outcomes": {}
        }

        # Determine the winning team
        winning_team = data.get('winning_team')

        # get rows for all clients
        steamIDs = []
        for client in data['clients']:
            steamIDs.append(client.get('steamID'))
        db_rows, not_found_steamids = get_rows_for_clients(steamIDs)

        # make new rows for clients not found
        if len(not_found_steamids) > 0:
            print(len(not_found_steamids), "clients not yet listed in rankings database.")
            names = []
            for steamid in not_found_steamids:
                for client in data['clients']:
                    if client.get('steamID') == steamid:
                        names.append(client.get('name'))
                        break
            print("Making", len(not_found_steamids) , "new clients in database.")
            make_rows_for_clients(not_found_steamids, names)

        # after the editting done previously, get final rows
        db_rows, not_found_steamids = get_rows_for_clients(steamIDs)
        print("Database rows:", db_rows)

        print("")
        print("RATING CALCULATION")
        print("------------------")
        # Process each client from the incoming JSON
        for client in data['clients']:
            steam_id = client.get('steamID')
            team = client.get('team')
            name = client.get('name')
            rating = db_rows.get(steam_id).get("elo")

            # Divide players into teams
            if team == 1:
                team1_ratings.append(rating)
            elif team == 2:
                team2_ratings.append(rating)

        # if one team has no members, give error
        if len(team1_ratings) == 0 or len(team2_ratings) == 0:
            print("One team has no members. Rankings will not be changed.")
            return "One team has no members. Rankings will not be changed."

        # Calculate team average ratings
        team1_rating = calculate_team_rating(team1_ratings)
        team2_rating = calculate_team_rating(team2_ratings)
        print("team1 rating:", team1_rating, ", team2 rating:", team2_rating)

        # Add team ratings to match data
        match_data["team_ratings"] = {
            "1": team1_rating,
            "2": team2_rating
        }

        # Calculate expected outcomes
        expected_team1 = calculate_expected_outcome(team1_rating, team2_rating)
        expected_team2 = calculate_expected_outcome(team2_rating, team1_rating)
        print("team1 winchance:", expected_team1 , ", team2 winchance:" , expected_team2)

        # Add expected outcomes to match data
        match_data["expected_outcomes"] = {
            "1": expected_team1,
            "2": expected_team2
        }

        print("ranking changes:")
        # Update ratings based on match outcome
        for client in data['clients']:
            steam_id = client.get('steamID')
            team = client.get('team')
            name = client.get('name')

            # did this player win? 1 = yes, 0 = no
            actual_outcome = 1 if team == winning_team else 0
            # the % chance that this player was expected to win
            expected_outcome = expected_team1 if team == 1 else expected_team2

            original = db_rows.get(steam_id).get("elo")
            nationality = db_rows.get(steam_id).get("nationality")
            new_rating = update_rating(original, expected_outcome, actual_outcome)

            # change rating in database
            change_rating(steam_id, name, new_rating, nationality)

            # Add player data to match data
            player_data = {
                "steamid": steam_id,
                "name": name,
                "old_rating": original,
                "new_rating": new_rating,
                "delta": new_rating - original,
                "nationality": nationality
            }

            match_data["teams"][str(team)].append(player_data)

            # Print the updated rating for each Steam ID
            print(f'Steam ID: {steam_id}, name: {name}, Rating: {original} -> {new_rating} (delta={new_rating-original})')

        # delete old entries
        delete_old_withThreads(steamIDs)

        # alerting bot with match data
        alert_bot(match_data)

        # Return a simple confirmation message
        print("Ratings updated. Check server console for details.")
        return "Ratings updated. Check server console for details."


if __name__ == '__main__':
    app.run(debug=True, port=14200)

