# Flights Data Connector

Flights Data Connector is for connectiong to a server database.

## Objective

The objective of this connector is to get the the Co2 emmition and the uniq number of traveler and uniq number of tickets per day. It achieves this by pushing the data receved form datbase through the Para Data Management API.

## Configuration

The configuration file includes the following parameters:

1. `FlightsData`: Contains the connection string to connect to DB.
2. `Entities`: All Entities with their ID on the Things Board for this task is Building "Smart Village" . If you want to change the Things Board environment, you should change the ID for the Building.
3. `API_URL`: Holds the connection parameters for the Para Data Management API.
4. `Telemetries`: Includes the key telemetry that will be pushed to the Para Data Management API.
