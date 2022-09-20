import os
import pandas as pd


def id_has_label(id):
    file = open("dataset/labeled_ids.txt", "r")
    labeled_ids_list = file.read().split()
    return id in labeled_ids_list



def read_plot_file(path):
    with open(path, "r") as f:
        colnames = ["lat", "long", "_0", "altitude", "_1", "date", "date_time"]
        df = pd.read_csv(f, skiprows=6, names=colnames)
        df = df.drop(columns=['_0', '_1'])

        if len(df.index) > 2506:
            # Empty dataframe
            return pd.DataFrame()

        return df


def openAllFiles():
    activites = dict()

    for root, dirs, files in os.walk("dataset/Data"):
        for name in dirs:
            user_id = name

            has_labels = id_has_label(user_id)
            #Opprett ny bruker i databasen

        for name in files:
            file_path = os.path.join(root, name)
            user_id = root.split("\\")[1]

            df = read_plot_file(file_path)
            # print(df)

            activity_id = user_id + "_" + name
            activites[activity_id] = {
                "user_id": user_id,
                "transportation_mode": "",
                "start_date_time": "",
                "end_date_time": ""
            }

            if name == "labels.txt":
                with open(file_path, encoding='utf-8') as f:
                    print(f)
                    #activity_id = user_id + "_" + name

                    #activites[activity_id]["transportation_mode"] = ""
                    #activites[activity_id]["start_date_time"] = ""
                    #activites[activity_id]["end_date_time"] = ""


openAllFiles()
