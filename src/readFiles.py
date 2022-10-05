import os
import pandas as pd
import time

def id_has_label(id):
    file = open("../dataset/labeled_ids.txt", "r")
    labeled_ids_list = file.read().split()
    return id in labeled_ids_list


def read_plot_file(path):
    with open(path) as f:
        colnames = ["lat", "long", "_0", "altitude", "_1", "date", "date_time"]
        df = pd.read_csv(f, skiprows=6, names=colnames)
        df = df.drop(columns=['_0', '_1'])

        if len(df.index) > 2500:
            # Empty dataframe
            return pd.DataFrame()

        return df


def read_labels_file(path):
    with open(path) as f:
        colnames = ["start_date_time", "end_date_time", "transportation_mode"]
        df = pd.read_csv(f, skiprows=1, names=colnames, sep="\t")
        return df


def open_all_files():
    users = dict()
    activities = dict()
    trackpoints = dict()

    number_of_files = 18_738

    current_file_index = 0

    for root, dirs, files in os.walk("../dataset/Data"):
        for name in dirs:
            if name == "Trajectory":
                continue

            user_id = name
            users[user_id] = id_has_label(user_id)

        for name in files:
            current_file_index += 1
            print("{} Preparing insert {:7.2f} % {:6,} / {:6,}".format(
                time.strftime("%H:%M:%S"),
                round(current_file_index / number_of_files * 100, 2),
                current_file_index,
                number_of_files
            )
            )

            file_path = os.path.join(root, name)

            if os.name == 'nt':
                user_id = root.split("\\")[2]
            else:
                user_id = root.split("/")[3]

            # if we are reading labels file
            if name == "labels.txt":
                df = read_labels_file(file_path)

                for _, row in df.iterrows():
                    stripped_start_date = row["start_date_time"].split(" ")[
                        0].replace("/", "")
                    stripped_start_time = row["start_date_time"].split(" ")[
                        1].replace(":", "")

                    activity_id = user_id + "_" + stripped_start_date + stripped_start_time

                    formatted_start_date = row["start_date_time"].replace(
                        "/", "-")
                    formatted_end_date = row["end_date_time"].replace("/", "-")

                    activities[activity_id] = {
                        "user_id": user_id,
                        "transportation_mode": row["transportation_mode"],
                        "start_date_time": formatted_start_date,
                        "end_date_time": formatted_end_date
                    }

            # else we are reading plot file
            else:
                activity_id = user_id + "_" + name.split(".")[0]

                df = read_plot_file(file_path)

                # if the activity does not exist we need to create it
                if not activity_id in activities and not df.empty:
                    start_date_time = df.iloc[0]["date"] + \
                        " " + df.iloc[0]["date_time"]
                    end_date_time = df.iloc[-1]["date"] + \
                        " " + df.iloc[-1]["date_time"]

                    activities[activity_id] = {
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
                        "altitude": "" if row["altitude"] == -777 else row["altitude"],
                        "date_days": row["date"].replace("-", ""),
                        "date_time": row["date"] + " " + row["date_time"]
                    }

    # prepare data for insertion, flatten the dictionaries into lists
    activities_list = [(k, *v.values()) for k, v in activities.items()]
    trackpoints_list = [(k, *v.values()) for k, v in trackpoints.items()]

    return users, activities_list, trackpoints_list