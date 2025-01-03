import pyodbc
import time
import datetime
from datetime import date, datetime, timedelta
import json
import requests
import pandas as pd
import pytz

# Define constants
CONFIG_FILENAME = "config.json"
HTTP_STATUS_OK = 200

#function to generate a token for validating the user to the data management API
def validate_user():

    url = "http://172.31.1.107:5000/authenticate"

    payload = json.dumps({
    "client_id": "para-be-connectors",
    "client_secret": "KujNaf8yZ5l1IjqEuf8rLaFlZc9bvHY0"
    })
    headers = {
    'Content-Type': 'application/json'
    }

    token = json.loads(requests.request("POST", url, headers=headers, data=payload).text)['access_token']

    return token

#function to open the config file and handling the error if the file not found or has invalid json format 
def load_config(file_config, t):
    try:
        with open(file_config, "r") as config_file:
            config = json.load(config_file)
            return config
    except FileNotFoundError:
        with open(err_file_name, "a") as err_f:
            err_f.write(f"{t.strftime('%I:%M:%S %p')} ### Configuration file '{file_config}' not found.")
        
    except json.JSONDecodeError:
        with open(err_file_name, "a") as err_f:
            err_f.write(f"{t.strftime('%I:%M:%S %p')} ### Invalid JSON format in '{file_config}'.")
        
#function that takes the te connection string and the quiry I need to exicute to return a list of the fetched data from SQL server DB. 
def get_data(filter_value, config, t):
    try:
        connection_string = config["FlightsData"]["connection_string"]
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        query = config["FlightsData"]["quiry"]

        # Execute SQL queries here
        cursor.execute(query, filter_value)

        data = cursor.fetchall()
        cursor.close()
        conn.close()

        # handling the condition if there is no data yet at the Requested Date
        if data[0][0] is None:
            return [(0, 0, 0)]
        return data
    except pyodbc.Error as e:
        with open(err_file_name, "a") as err_f:
            err_f.write(f"{t.strftime('%I:%M:%S %p')} ### Error connecting to SQL Server: {e}")
        pass
            # time.sleep(30)
            # get_data(filter_value, config, Cairo_time_now)

# function to get the monthly date from DB by executing the query
def get_mon_data(first_day_str, last_day_str, config, t):
    try:
        connection_string = config["FlightsData"]["connection_string"]
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        query = config["FlightsData"]["monqyery"]

        # Execute SQL queries here
        cursor.execute(query, first_day_str, last_day_str)

        data = cursor.fetchall()
        cursor.close()
        conn.close()

        # handling the condition if there is no data yet at the Requested Date
        if data[0][0] is None:
            return [(0, 0, 0)]
        return data
    except pyodbc.Error as e:
        with open(err_file_name, "a") as err_f:
            err_f.write(f"{t.strftime('%I:%M:%S %p')} ### Error connecting to SQL Server: {e}")
        pass
        
# Function that push the telemetries to the data manegment API
def connect_to_para(token, tb_id, timestamp, co2_value, travelers_value, tickets_value, config,t,mon=False):
    url = config['API_URL']['url']
    if mon == False:

        payload = json.dumps([
            {
            "entityName": tb_id,
            "values": [
                {
                "ts": timestamp,
                "values": {
                    config["Telemetries"]["CO2imm"]: co2_value,
                    config["Telemetries"]["travelers"]: travelers_value,
                    config["Telemetries"]["teckets"]: tickets_value
                }
                }
            ]
            }
        ])
    else:
        payload = json.dumps([
            {
            "entityName": tb_id,
            "values": [
                {
                "ts": timestamp,
                "values": {
                    config["Telemetries"]["CO2immon"]: co2_value,
                    config["Telemetries"]["travelersmon"]: travelers_value,
                    config["Telemetries"]["tecketsmon"]: tickets_value
                }
                }
            ]
            }
        ])
    headers = {'Content-Type': 'application/json',
               'Authorization': f'Bearer {token}'}

    try:
        response = requests.post(url, headers=headers, data=payload)
        #check the respons code to catch the error if it is not = 200
        if response.status_code != 201:
            with open(err_file_name, "a") as err_f:
                err_f.write(f"{t.strftime('%I:%M:%S %p')} ### The Entity with ID: {tb_id} did not post the Telemetries Values to the following URL: {url}")
            
    except requests.exceptions.RequestException as e:
        with open(err_file_name, "a") as err_f:
            err_f.write(f"{t.strftime('%I:%M:%S %p')} ### Error sending data to the API: {e}")
        
def previous_day_midnight(timestamp):
    # Convert timestamp to a datetime object
        dt = datetime.fromtimestamp(timestamp / 1000)
       
        # Subtract one day (86400 seconds) from the datetime object
        previous_day_dt = dt - timedelta(days=1)
       
        # Set time to midnight (12:00 AM)
        previous_day_midnight_dt = previous_day_dt.replace(hour=0, minute=0, second=0, microsecond=0)
       
        # Convert back to timestamp (in milliseconds)
        previous_day_midnight_ts = int(previous_day_midnight_dt.timestamp()) * 1000
       
        return previous_day_midnight_ts

def last_update(token, ts):
    url = config['API_URL']['url']
    payload = json.dumps([
        {
            "entityName": "FlightsDataConnector",
            "values": [
                {
                    "ts": ts,
                    "values": {
                        "LSTUPT_MS_FLIGHT": int(ts),
                        
                    }
                }
            ]
        }
    ])
    headers = {'Content-Type': 'application/json',
               'Authorization': f'Bearer {token}'}

    try:
        response = requests.post(url, headers=headers, data=payload)
        
            
    except requests.exceptions.RequestException as e:
       pass

if __name__ == "__main__":  

    while True:

        # Get the current datetime in the Africa/Cairo timezone
        Cairo_time_now = datetime.now(pytz.timezone('Africa/Cairo'))
        filter_value = Cairo_time_now.strftime("%Y-%m-%d 00:00:00")
        # Get the first day of the month
        first_day = Cairo_time_now.replace(day =1)
        # Calculate the last day of the previous month
        last_day_of_previous_month = first_day - timedelta(days=1)
        # Calculate the first day of the previous month
        first_day_of_previous_month = last_day_of_previous_month.replace(day =1)

        # Format the first and last day as strings
        first_day_str = first_day_of_previous_month.strftime("%Y-%m-%d 00:00:00")
        last_day_str = last_day_of_previous_month.strftime("%Y-%m-%d 00:00:00")
        first_day_mon = int(first_day.strftime("%d"))
        timestamp = pd.Timestamp(Cairo_time_now).floor('10T').timestamp() * 1000

        # Set up error log file
        log_file = f"ERR_{date.today()}"
        err_file_name = f"logs/{log_file}"
        err_f = open(err_file_name, "a")

        config = load_config(CONFIG_FILENAME,Cairo_time_now)
        tb_id = config["Entities"]["srckey"]

        column_name = "RequestedDate"

        data = get_data(filter_value, config, Cairo_time_now)
        try:
            flight_co2_emissions = float(data[0][0])
            unique_traveler_count = data[0][1]
            unique_tickets_count = data[0][2]
                
        except Exception as err:
            with open(err_file_name, "a") as err_f:
                err_f.write(f"{Cairo_time_now.strftime('%I:%M:%S %p')} ### There is failure response for flights data where {column_name} = {filter_value} \n")
            continue
        try:
            token = validate_user()
            connect_to_para(token, tb_id, timestamp, flight_co2_emissions, unique_traveler_count, unique_tickets_count, config, Cairo_time_now)
        except Exception as e:
            with open(err_file_name, "a") as err_f:
                err_f.write(f"{Cairo_time_now.strftime('%I:%M:%S %p')} ### Error sending data to the API: {e}")

        last_update(token, timestamp)
        with open(err_file_name, "a") as err_f:
            err_f.write(f"{Cairo_time_now.strftime('%I:%M:%S %p')} ### There is succeful response for flights data where {column_name} = {filter_value}: \n Co2_Emission: {flight_co2_emissions} \n Uniq_Travelers: {unique_traveler_count} \n Uniq_Tickets: { unique_tickets_count}\n")

        time.sleep(600)
        if date.today().day == first_day_mon:
            mon_data =get_mon_data(first_day_str, last_day_str, config, Cairo_time_now)

            try:
                flight_co2_emissions_mon = float(mon_data[0][0])
                unique_traveler_count_mon = mon_data[0][1]
                unique_tickets_count_mon = mon_data[0][2]            
            except Exception as err:
                with open(err_file_name, "a") as err_f:
                    err_f.write(f"{Cairo_time_now.strftime('%I:%M:%S %p')} ### There is failure response for flights data where {column_name} = {last_day_str} \n")
                continue

            timest= previous_day_midnight(timestamp)
            connect_to_para(token, tb_id, timest, flight_co2_emissions_mon, unique_traveler_count_mon, unique_tickets_count_mon, config, Cairo_time_now, mon = True)
            with open(err_file_name, "a") as err_f:
                err_f.write(f"{Cairo_time_now.strftime('%I:%M:%S %p')} ### There is succeful response for monthly flights data where {column_name} between {first_day_str} and {last_day_str}: \n Co2_Emission: {flight_co2_emissions} \n Uniq_Travelers: {unique_traveler_count} \n Uniq_Tickets: { unique_tickets_count}\n")