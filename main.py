import requests
import mysql.connector

# Connect to MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    database="F1Spectrum"
)

mycursor = mydb.cursor()

# Fetch data from Ergast API
response = requests.get("http://ergast.com/api/f1/2023.json")
data = response.json()

# Insert data into Seasons table
season_year = 2023
mycursor.execute("INSERT INTO Seasons (Year) VALUES (%s)", (season_year,))
mydb.commit()

# Get the inserted SeasonID
mycursor.execute("SELECT SeasonID FROM Seasons WHERE Year = %s", (season_year,))
season_id = mycursor.fetchone()[0]

# Insert data into Races and Circuits tables
for race in data['MRData']['RaceTable']['Races']:
    circuit = race['Circuit']

    # Check if circuit already exists
    mycursor.execute("SELECT CircuitID FROM Circuits WHERE Name = %s", (circuit['circuitName'],))
    circuit_id = mycursor.fetchone()

    # If circuit doesn't exist, insert it
    if circuit_id is None:
        mycursor.execute("INSERT INTO Circuits (Name, Location) VALUES (%s, %s)",
                         (circuit['circuitName'], circuit['Location']['locality']))
        mydb.commit()
        circuit_id = mycursor.lastrowid

    # Insert race
    mycursor.execute("INSERT INTO Races (SeasonID, CircuitID, Date, Round) VALUES (%s, %s, %s, %s)",
                     (season_id, circuit_id, race['date'], race['round']))
    mydb.commit()



# And so on for Constructors, Drivers, Results, and Telemetry tables...
