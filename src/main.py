import argparse
import time
from datetime import datetime
from insertData import insert_data
from queryExecution import QueryExecution
import os


def init_db():
    """
    Initialize the database
    """
    # Format the current time
    FMT = '%H:%M:%S'
    start_datetime = time.strftime(FMT)
    insert_data()
    end_datetime = time.strftime(FMT)
    # Calculate the time difference
    total_datetime = datetime.strptime(
        end_datetime, FMT) - datetime.strptime(start_datetime, FMT)
    print(
        f"Started: {start_datetime}\nFinished: {end_datetime}\nTotal: {total_datetime}")

def dataset_is_present() -> bool:
    if os.path.exists("../dataset"):
        return True
    else:
        return False


def main(should_init_db=False):
    if should_init_db:
        # Testing if dataset is in the correct folder
        if dataset_is_present():
            init_db()
        else:
            print("Dataset not found. Needs to be located in the root of the project folder, and be named 'dataset'")
            return

    query = QueryExecution()
    print("\n-------- Query 1 ----------")
    query.sum_user_activity_trackpoint()

    print("\n-------- Query 2 ----------")
    query.average_number_of_activities_per_user()

    print("\n-------- Query 3 ----------")
    query.top_twenty_users()

    print("\n-------- Query 4 ----------")
    query.users_taken_taxi()

    print("\n-------- Query 5 ----------")
    query.activity_transport_mode_count()

    print("\n-------- Query 6 ----------")
    query.year_with_most_activities()

    print("\n-------- Query 7 ----------")
    query.total_distance_in_km_walked_in_2008_by_userid_112()

    print("\n-------- Query 8 ----------")
    print("This one takes a while...\n")
    query.top_20_users_gained_most_altitude_meters()

    print("\n-------- Query 9 ----------")
    print("This one takes a while...\n")
    query.invalid_activities_per_user()

    print("\n-------- Query 10 ----------")
    query.users_tracked_activity_in_the_forbidden_city_beijing()

    print("\n-------- Query 11 ----------")
    query.users_registered_transportation_mode_and_their_most_used_transportation_mode()

    # Close the connection after all queries are executed
    query.connection.close_connection()


if __name__ == "__main__":
    # Enables flag to initialize database
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--init_database",
                        action="store_true", help="Initialize the database")
    args = parser.parse_args()

    main(args.init_database)
