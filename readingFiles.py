from io import StringIO
import os
import pandas as pd
import itertools


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
    users = dict()
    activites = dict()
    trackpoints = dict()

    for root, dirs, files in os.walk("dataset/Data"):
        for name in dirs:
            user_id = name

            users[user_id] = {
                "has_labels": id_has_label(user_id),
            }

            # insert user into database

        for name in files[:2]:
            file_path = os.path.join(root, name)
            print("processing file:", file_path)
            user_id = root.split("/")[2]

            # if we are reading labels file
            if name == "labels.txt":
                df = read_labels_file(file_path)

                for _, row in df.iterrows():
                    stripped_start_date = row["start_date_time"].split(" ")[0].replace("/", "")
                    stripped_start_time = row["start_date_time"].split(" ")[1].replace(":", "")
                    activity_id = user_id + "_" + stripped_start_date + stripped_start_time
                    
                    formatted_start_date = row["start_date_time"].replace("/", "-")
                    formatted_end_date = row["end_date_time"].replace("/", "-")

                    activites[activity_id] = {
                        "user_id": user_id,
                        "transportation_mode": row["transportation_mode"],
                        "start_date_time": formatted_start_date,
                        "end_date_time": formatted_end_date
                    }

            # if we are reading plot file
            else:
                activity_id = user_id + "_" + name.split(".")[0]
                df = read_plot_file(file_path)

                # if the activity does not exist we need to create it
                if not activity_id in activites and not df.empty:
                    start_date_time = df.iloc[0]["date"] + " " + df.iloc[0]["date_time"]
                    end_date_time = df.iloc[-1]["date"] + " " + df.iloc[-1]["date_time"]

                    activites[activity_id] = {
                        "user_id": user_id,
                        "transportation_mode": "",
                        "start_date_time": start_date_time,
                        "end_date_time": end_date_time
                    }
                
                for _, row in df.iterrows():
                    stripped_start_date = row["date"].replace("/", "")
                    stripped_start_time = row["date_time"].replace(":", "")

                    trackpoint_id = activity_id + "_" + stripped_start_date + stripped_start_time

                    trackpoints[trackpoint_id] = {
                        "activity_id": activity_id,
                        "lat": row["lat"],
                        "lon": row["long"],
                        "altitude": row["altitude"],
                        "date_days": row["date"],
                        "date_time": row["date_time"]
                    }


    print("\nusers")
    print(str(dict(itertools.islice(users.items(),4))).replace("}, ", "},\n"))
    
    print("\nactivites")
    print(str(dict(itertools.islice(activites.items(),4))).replace("}, ", "},\n"))
    
    print("\ntrackpoints")
    print(str(dict(itertools.islice(trackpoints.items(),4))).replace("}, ", "},\n"))



openAllFiles()
