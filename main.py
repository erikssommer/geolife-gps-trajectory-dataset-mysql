from queryExecution import QueryExecution

def main():
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
    query.top_20_users_gained_most_altitude_meters()
    print("\n-------- Query 9 ----------")
    print("This one takes a while...")
    query.invalid_activities_per_user()
    print("\n-------- Query 10 ----------")
    query.users_tracked_activity_in_the_forbidden_city_beijing()
    print("\n-------- Query 11 ----------")
    query.users_registered_transportation_mode_and_their_most_used_transportation_mode()


if __name__ == "__main__":
    main()