import time
from DbConnector import DbConnector
from readFiles import open_all_files


def clear_db(cursor):
    """
    Clears all data from the database
    """

    print(
        f"\n{time.strftime('%H:%M:%S')} Clearing existing trackpoints from database...")
    cursor.execute("DELETE FROM TrackPoint")

    print(
        f"\n{time.strftime('%H:%M:%S')} Clearing existing activities from database...")
    cursor.execute("DELETE FROM Activity")

    print(f"\n{time.strftime('%H:%M:%S')} Clearing existing users from database...\n")
    cursor.execute("DELETE FROM User")


def insert_data():
    """
    Insert data into the database
    """
    
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    # Starts with clearing the database
    clear_db(cursor)

    # Opens all files and returns a dictionary with all the data
    users, activities_list, trackpoints_list = open_all_files()

    # insert data into database
    print(f"\n{time.strftime('%H:%M:%S')} inserting {len(users)} users...")
    cursor.executemany(
        "INSERT INTO User (id, has_labels) VALUES (%s, %s)", list(users.items()))
    db_connection.commit()
    print(f"\n{time.strftime('%H:%M:%S')} inserted {len(users)} users")

    print(f"\n{time.strftime('%H:%M:%S')} inserting {len(activities_list)} activities...")
    cursor.executemany(
        "INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)", activities_list)
    db_connection.commit()
    print(f"\n{time.strftime('%H:%M:%S')} inserted {len(activities_list)} activities")

    # Iterates through all trackpoints and inserts them into the database in batches of 1000
    counter = 0
    increment = 1000
    for i in range(0, int(len(trackpoints_list) / increment)):
        counter += increment
        print("{} Inserting trackpoints {:7.2f} % {:9,} / {:9,}".format(
            time.strftime("%H:%M:%S"),
            round(i / (len(trackpoints_list) / increment) * 100, 2),
            counter,
            len(trackpoints_list)
        ))
        trackpoints_string = str(
            trackpoints_list[counter:counter + increment]).strip("[]") + ";"
        cursor.execute(
            f"INSERT IGNORE INTO TrackPoint (id, activity_id, lat, lon, altitude, date_days, date_time) VALUES {trackpoints_string}")
        db_connection.commit()

    # We need to insert the rest of the trackpoints if the number of trackpoints is not divisible by 1000
    print("{} Inserting trackpoints {:7.2f} % {:9,} / {:9,}".format(
        time.strftime("%H:%M:%S"),
        100.00,
        len(trackpoints_list),
        len(trackpoints_list)
    )
    )
    trackpoints_string = str(trackpoints_list[:len(
        trackpoints_list) % increment]).strip("[]")
    cursor.execute(
        f"INSERT IGNORE INTO TrackPoint (id, activity_id, lat, lon, altitude, date_days, date_time) VALUES {trackpoints_string}")
    db_connection.commit()

    # Close database connection after all data is inserted
    connection.close_connection()

    print("\nInserted {:,} trackpoints".format(len(trackpoints_list)))
