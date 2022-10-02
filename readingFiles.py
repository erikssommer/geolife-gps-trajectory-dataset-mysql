import os
import pandas as pd
import time

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
    activities = set()
    trackpoints = dict()

    number_of_files = 0

    for root, _, files in os.walk("dataset/Data"):
        for file_name in files:
            if file_name.endswith(".plt"):
                file_path = os.path.join(root, file_name)
                df = pd.read_csv(file_path, skiprows=1, sep="\t")
                if len(df.index) <= 2506:
                    number_of_files += 1
            else:
                number_of_files += 1

    current_file_index = 0

    for root, dirs, files in os.walk("dataset/Data"):
        for name in dirs:
            if name == "Trajectory":
                continue

            user_id = name
            # users[user_id] = id_has_label(user_id)

            cursor.execute("INSERT INTO User (id, has_labels) VALUES (%s, %s)", (user_id, id_has_label(user_id)))
        
        # for name in files[:6]: #limit to 2 files per folder for faster testing
        for name in files:
            current_file_index += 1
            print("%s Progress %6s %% - %5s / %5s" %
                (
                    time.strftime("%Y-%m-%d %H:%M:%S"),
                    round(current_file_index / number_of_files * 100, 2),
                    current_file_index,
                    number_of_files
                )
            )

            file_path = os.path.join(root, name)
            user_id = root.split("/")[2]

            # if we are reading labels file
            if name == "labels.txt":
                df = read_labels_file(file_path)

                for _, row in df.iterrows():
                    stripped_start_date = row["start_date_time"].split(" ")[0].replace("/", "")
                    stripped_start_time = row["start_date_time"].split(" ")[1].replace(":", "")
                    
                    stripped_end_date = row["end_date_time"].split(" ")[0].replace("/", "")
                    stripped_end_time = row["end_date_time"].split(" ")[1].replace(":", "")

                    activity_id = user_id + "_" + stripped_start_date + stripped_start_time + "_" + stripped_end_date + stripped_end_time #+ "_" + row["transportation_mode"]
                    # activity_id = stripped_start_date + stripped_start_time
                    
                    formatted_start_date = row["start_date_time"].replace("/", "-")
                    formatted_end_date = row["end_date_time"].replace("/", "-")

                    activity = (
                        activity_id,
                        user_id,
                        row["transportation_mode"],
                        formatted_start_date,
                        formatted_end_date
                    )
                    activities.add(activity_id)
                    cursor.execute("INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)", activity)
                    continue

                    # activities[activity_id] = {
                    #     "user_id": user_id,
                    #     "transportation_mode": row["transportation_mode"],
                    #     "start_date_time": formatted_start_date,
                    #     "end_date_time": formatted_end_date
                    # }

                db_connection.commit()

            # else we are reading plot file
            else:
                activity_id = user_id + "_" + name.split(".")[0]
                # activity_id = name.split(".")[0]
                df = read_plot_file(file_path)

                # if the activity does not exist we need to create it
                if not activity_id in activities and not df.empty:
                    start_date_time = df.iloc[0]["date"] + " " + df.iloc[0]["date_time"]
                    end_date_time = df.iloc[-1]["date"] + " " + df.iloc[-1]["date_time"]

                    activity = (
                        activity_id,
                        user_id,
                        "",
                        start_date_time,
                        end_date_time
                    )
                    cursor.execute("INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)", activity)
                    # print(f"Inserting activity %s with %4s trackpoints" % (activity_id, len(df.index)))

                    # activities[activity_id] = {
                    #     "user_id": user_id,
                    #     "transportation_mode": "",
                    #     "start_date_time": start_date_time,
                    #     "end_date_time": end_date_time
                    # }
                
                for _, row in df.iterrows():
                    stripped_start_date = row["date"].replace("/", "")
                    stripped_start_time = row["date_time"].replace(":", "")

                    trackpoint_id = activity_id + "_" + stripped_start_date + stripped_start_time
                    # trackpoint_id = stripped_start_date + stripped_start_time

                    trackpoint = (
                        trackpoint_id,
                        activity_id,
                        row["lat"],
                        row["long"],
                        None if row["altitude"] == -777 else row["altitude"],
                        row["date"].replace("-", ""),
                        row["date"] + " " + row["date_time"]
                    )

                    # cursor.execute("INSERT INTO TrackPoint (id, activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE", trackpoint)
                    cursor.execute("INSERT IGNORE INTO TrackPoint (id, activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s, %s)", trackpoint)
                    # print(f"Inserting trackpoint {trackpoint_id}")
                    continue
                    
                    trackpoints[trackpoint_id] = {
                        "activity_id": activity_id,
                        "lat": row["lat"],
                        "lon": row["long"],
                        "altitude": row["altitude"],
                        "date_days": row["date"].replace("-", ""),
                        "date_time": row["date"] + " " + row["date_time"]
                    }

        db_connection.commit()

    db_connection.commit()

    # print("\nusers")
    # print(list(users.items())[:5])

    # # prepare data for insertion, flatten the dictionaries into lists
    # activities_list = [(k, *v.values()) for k, v in activities.items()]
    # print("\nactivities_list")
    # print(activities_list[:5])

    # trackpoints_list = [(k, *v.values()) for k, v in trackpoints.items()]
    # print("\ntrackpoints_list")
    # print(trackpoints_list[:5])

    # # insert data into database

    # print(f"\n {time.strftime('%Y-%m-%d %H:%M')} inserting {len(users)} users...")
    # cursor.executemany("INSERT INTO User (id, has_labels) VALUES (%s, %s)", list(users.items()))
    # db_connection.commit()
    # print(f"\n {time.strftime('%Y-%m-%d %H:%M')} inserted {cursor.rowcount} users")

    # print(f"\n {time.strftime('%Y-%m-%d %H:%M')} inserting {len(activities_list)} activities...")
    # cursor.executemany("INSERT INTO Activity (id, user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s, %s)", activities_list)
    # db_connection.commit()
    # print(f"\n {time.strftime('%Y-%m-%d %H:%M')} inserted {cursor.rowcount} activities")

    # print(f"\n {time.strftime('%Y-%m-%d %H:%M')} inserting {len(trackpoints_list)} trackpoints...")
    # cursor.executemany("INSERT INTO TrackPoint (id, activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s, %s)", trackpoints_list)
    # db_connection.commit()
    # print(f"\n {time.strftime('%Y-%m-%d %H:%M')} inserted {cursor.rowcount} trackpoints")

start_datetime = time.strftime("%Y-%m-%d %H:%M:%S")
openAllFiles()
end_datetime = time.strftime("%Y-%m-%d %H:%M:%S")
print(f"Started: {start_datetime}\nFinished: {end_datetime}")
