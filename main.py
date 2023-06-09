import requests
import mysql.connector

# Connect to MySQL
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    database="F1Spectrum"
)

mycursor = mydb.cursor(buffered=True)

# Fetch data from Ergast API
response = requests.get("http://ergast.com/api/f1/2022.json")
data = response.json()

# Insert data into Seasons table
season_year = 2022
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
    else:
        circuit_id = circuit_id[0]  # Extract single value from tuple

    # Insert race
    mycursor.execute("INSERT INTO Races (SeasonID, CircuitID, Date, Round) VALUES (%s, %s, %s, %s)",
                     (season_id, circuit_id, race['date'], race['round']))
    mydb.commit()

# Fetch constructors data from Ergast API
response = requests.get("http://ergast.com/api/f1/2022/constructors.json")
data = response.json()

# Insert data into Constructors table
for constructor in data['MRData']['ConstructorTable']['Constructors']:
    mycursor.execute("INSERT INTO Constructors (Name, Nationality) VALUES (%s, %s)",
                     (constructor['name'], constructor['nationality']))
    mydb.commit()

# Fetch drivers data from Ergast API
response = requests.get("http://ergast.com/api/f1/2022/results.json")
data = response.json()

# Insert data into Drivers table
for race in data['MRData']['RaceTable']['Races']:
    for result in race['Results']:
        driver = result['Driver']
        constructor = result['Constructor']
        # Get the constructor id for the driver
        mycursor.execute("SELECT ConstructorID FROM Constructors WHERE Name = %s", (constructor['name'],))
        constructor_id = mycursor.fetchone()[0]

        mycursor.execute("INSERT INTO Drivers (ConstructorID, Name, Nationality) VALUES (%s, %s, %s)",
                         (constructor_id, driver['givenName'] + ' ' + driver['familyName'], driver['nationality']))
        mydb.commit()



# Fetch results data from Ergast API
response = requests.get("http://ergast.com/api/f1/2022/results.json")
data = response.json()

# Insert data into Results table
for race in data['MRData']['RaceTable']['Races']:
    # Get the race id
    mycursor.execute("SELECT RaceID FROM Races WHERE Date = %s", (race['date'],))
    race_id = mycursor.fetchone()[0]

    for result in race['Results']:
        # Get the driver id and constructor id
        mycursor.execute("SELECT DriverID FROM Drivers WHERE Name = %s",
                         (result['Driver']['givenName'] + ' ' + result['Driver']['familyName'],))
        driver_id = mycursor.fetchone()[0]

        mycursor.execute("SELECT ConstructorID FROM Constructors WHERE Name = %s", (result['Constructor']['name'],))
        constructor_id = mycursor.fetchone()[0]

        mycursor.execute(
            "INSERT INTO Results (RaceID, DriverID, ConstructorID, GridPosition, FinalPosition, Points) VALUES (%s, %s, %s, %s, %s, %s)",
            (race_id, driver_id, constructor_id, result['grid'], result['position'], result['points']))
        mydb.commit()
