import os
import pandas as pd
import itertools

from DbConnector import DbConnector


def id_has_label(id):
    file = open("dataset/labeled_ids.txt", "r")
    labeled_ids_list = file.read().split()
    return id in labeled_ids_list



def read_plot_file(path):
    with open(path) as f:
        colnames = ["lat", "long", "_0", "altitude", "_1", "date", "date_time"]
        df = pd.read_csv(f, skiprows=6, names=colnames)
        df = df.drop(columns=['_0', '_1'])

        if len(df.index) > 2506:
            # Empty dataframe
            return pd.DataFrame()

        return df


def read_labels_file(path):
    with open(path) as f:
        colnames = ["start_date_time", "end_date_time", "transportation_mode"]
        df = pd.read_csv(f, skiprows=1, names=colnames, sep="\t")
        return df


def openAllFiles():
    connection = DbConnector()
    db_connection = connection.db_connection
    cursor = connection.cursor

    users = dict()
    activities = dict()
    trackpoints = dict()

    for root, dirs, files in os.walk("dataset/Data"):
        for name in dirs:
            user_id = name
            if user_id == "Trajectory":
                continue

            users[user_id] = id_has_label(user_id)

        for name in files[:2]: #limit to 2 files per folder for testing
        # for name in files:
            file_path = os.path.join(root, name)
            print("processing file:", file_path)
            user_id = root.split("/")[2]

            # if we are reading labels file
            if name == "labels.txt":
                df = read_labels_file(file_path)

                for _, row in df.iterrows():
                    stripped_start_date = row["start_date_time"].split(" ")[0].replace("/", "")
                    stripped_start_time = row["start_date_time"].split(" ")[1].replace(":", "")
                    # activity_id = user_id + "_" + stripped_start_date + stripped_start_time
                    activity_id = stripped_start_date + stripped_start_time
                    
                    formatted_start_date = row["start_date_time"].replace("/", "-")
                    formatted_end_date = row["end_date_time"].replace("/", "-")

                    activities[activity_id] = {
                        "user_id": user_id,
                        "transportation_mode": row["transportation_mode"],
                        "start_date_time": formatted_start_date,
                        "end_date_time": formatted_end_date
                    }

            # if we are reading plot file
            else:
                # activity_id = user_id + "_" + name.split(".")[0]
                activity_id = name.split(".")[0]
                df = read_plot_file(file_path)

                # if the activity does not exist we need to create it
                if not activity_id in activities and not df.empty:
                    start_date_time = df.iloc[0]["date"] + " " + df.iloc[0]["date_time"]
                    end_date_time = df.iloc[-1]["date"] + " " + df.iloc[-1]["date_time"]

                    activities[activity_id] = {
                        "user_id": user_id,
                        "transportation_mode": "",
                        "start_date_time": start_date_time,
                        "end_date_time": end_date_time
                    }
                
                for _, row in df.iterrows():
                    stripped_start_date = row["date"].replace("/", "")
                    stripped_start_time = row["date_time"].replace(":", "")

                    # trackpoint_id = activity_id + "_" + stripped_start_date + stripped_start_time
                    trackpoint_id = stripped_start_date + stripped_start_time

                    trackpoints[trackpoint_id] = {
                        "activity_id": activity_id,
                        "lat": row["lat"],
                        "lon": row["long"],
                        "altitude": row["altitude"],
                        "date_days": row["date"].replace("-", ""),
                        "date_time": row["date"] + " " + row["date_time"]
                    }


    print("\nusers")
    print(list(users.items())[:5])

    # prepare data for insertion, flatten the dictionaries into lists
    activities_list = [(k, *v.values()) for k, v in activities.items()]
    print("\nactivities_list")
    print(activities_list[:5])

    trackpoints_list = [(k, *v.values()) for k, v in trackpoints.items()]
    print("\ntrackpoints_list")
    print(trackpoints_list[:5])

    print("\ninserting users...")

    # insert data into database
    cursor.executemany("INSERT INTO User (id, has_labels) VALUES (%s, %s)", list(users.items()))
    db_connection.commit()
    print("inserted %s users" % cursor.rowcount)

    print("\ninserting activities...")
    cursor.executemany("INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)", activities_list)
    db_connection.commit()
    print("inserted %s activities" % cursor.rowcount)

    print("\ninserting trackpoints...")
    cursor.executemany("INSERT INTO TrackPoint (id, activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s, %s)", trackpoints_list)
    db_connection.commit()
    print("inserted %s trackpoints" % cursor.rowcount)

openAllFiles()
