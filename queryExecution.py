from DbConnector import DbConnector

class QueryExecution:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Query 1 - How many users, activities and trackpoints are there in the dataset?
    def sum_user_activity_trackpoint(self):
        query = """
            SELECT 
            (SELECT COUNT(id) FROM User) AS user_sum,
            (SELECT COUNT(id) FROM Activity) AS activity_sum,
            (SELECT COUNT(id) FROM TrackPoint) AS trackpoint_sum
        """
        res = self.execute_query(query)
        user_sum = res[0][0]
        activity_sum = res[0][1]
        trackpoint_sum = res[0][2]
        print("There are {} users, {} activities and {} trackpoints in the dataset".format(user_sum, activity_sum, trackpoint_sum))

    # Query 2 - Find the average number of activities per user.
    def average_number_of_activities_per_user(self):
        query = """
            SELECT AVG(count) AS average_number_of_activities
            FROM (
                SELECT user_id, COUNT(*) AS count
                FROM geolife.Activity
                GROUP BY user_id
            ) AS user_activity_count
        """
        res = self.execute_query(query)
        average = res[0][0]
        print("The average number of activities per user is {}".format(average))

    # Query 3 - Find the top 20 users with the highest number of activities.
    def top_twenty_users(self):
        query = """
            SELECT user_id, COUNT(*) AS num_activities 
            FROM Activity
            GROUP BY user_id
            ORDER BY num_activities DESC
            LIMIT 20
        """
        res = self.execute_query(query)

        for user in res:
            print("User {} has {} activities".format(user[0], user[1]))

    # Query 4 - Find all users who have taken a taxi.
    def users_taken_taxi(self):
        query = """
            SELECT DISTINCT user_id
            FROM Activity
            WHERE transportation_mode = "taxi"
        """
        res = self.execute_query(query)

        for user in res:
            print("User {} has taken a taxi".format(user[0]))

    # Query 5 - Find all types of transportation modes and count how many activities that are tagged with these transportation mode labels. Do not count the rows where the mode is null.
    def activity_transport_mode_count(self):
        query = """
            SELECT transportation_mode, COUNT(transportation_mode) as count
            FROM geolife.Activity
            WHERE transportation_mode != ""
            GROUP BY transportation_mode
        """
        res = self.execute_query(query)
        for row in res:
            print("There are {} activities tagged with {}".format(row[1], row[0]))
    
    # Query 6
    def year_with_most_activities(self):
        # Find the year with the most activities.
        query6a = """
            SELECT YEAR(start_date_time) AS year, COUNT(id) AS count
            FROM Activity
            GROUP BY year
            LIMIT 1
        """
        res6a = self.execute_query(query6a)
        for row in res6a:
            print("The year {} has the most activities with {} activities".format(row[0], row[1]))

        # Query 6b - Is this also the year with most recorded hours?
        query6b = """
            SELECT YEAR(start_date_time) AS year, SEC_TO_TIME(SUM(TIME_TO_SEC(TIMEDIFF(end_date_time, start_date_time)))) AS duration
            FROM Activity
            GROUP BY year
            LIMIT 1
        """
        res6b = self.execute_query(query6b)
        for row in res6b:
            print("The year {} has the most recorded hours with {} hours".format(row[0], row[1]))
        
        if res6b[0][0] == res6a[0][0]:
            print("Yes, this is also the year with most recorded hours!")
        else:
            print("No, this is not the year with most recorded hours")
        
        

    # Query 7 - Find the total distance (in km) walked in 2008, by user with id = 112
    def total_distance_in_km_walked_in_2008_by_userid_112(self):
        query = """
            SELECT SUM(distance) AS total_distance
            FROM (
                SELECT Activity.id, Activity.user_id, Activity.transportation_mode, 
                (6371 * acos(cos(radians(52.520008)) * cos(radians(TrackPoint.lat)) * 
                cos(radians(TrackPoint.lon) - radians(13.404954)) + sin(radians(52.520008)) * 
                sin(radians(TrackPoint.lat)))) AS distance
                FROM Activity
                JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
                WHERE Activity.user_id = 112
                AND Activity.transportation_mode = "walk"
                AND YEAR(Activity.start_date_time) = 2008
            ) AS distance_table
        """
        res = self.execute_query(query)
        print("The total distance walked in 2008 by user 112 is {} km".format(res[0][0]))

    # Query 8 - Find the top 20 users who have gained the most altitude meters.
    def top_20_users_gained_most_altitude_meters(self):
        query = """
            SELECT Activity.user_id, SUM(TrackPoint.altitude) as "total meters gained per user"
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
            GROUP BY Activity.user_id
            ORDER BY SUM(TrackPoint.altitude) DESC
            LIMIT 20
        """
        res = self.execute_query(query)
        count = 0
        for row in res:
            count += 1
            print("{}: User {} has gained {} meters".format(count, row[0], row[1]))

    # Query 9 - Find all users who have invalid activities, and the number of invalid activities per user
    def invalid_activities_per_user(self):
        query = """
            SELECT user_id, COUNT(DISTINCT activity_id) AS fault_activity_amount
                FROM(
                    SELECT MINUTE(TIMEDIFF(startTime, prev_time)) AS time_diff, user_id, activity_id, prev_a_id
                        FROM(
                        SELECT t1.date_time AS startTime, LAG(t1.date_time) OVER(ORDER BY date_time) AS prev_time, user_id, activity_id, LAG(t1.activity_id) OVER(ORDER BY date_time) AS prev_a_id
                        FROM TrackPoint t1
                        INNER JOIN Activity ON Activity.id = t1.activity_id
                        ) AS time_table
                    ) AS diff_table
                WHERE time_diff > 5
                AND activity_id = prev_a_id
                GROUP BY user_id
                ORDER BY user_id ASC
        """

        res = self.execute_query(query)
        for row in res:
            print("User {} has {} invalid activities".format(row[0], row[1]))


    # Query 10 - Find the users who have tracked an activity in the Forbidden City of Beijing.
    def users_tracked_activity_in_the_forbidden_city_beijing(self):
        query = """
            SELECT user_id, COUNT(TrackPoint.id) as 'trackpoints in forbidden city'
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
            WHERE lat >= 39.916 AND lat < 39.917
            AND lon >= 116.397 AND lon < 116.398
            GROUP BY Activity.user_id
        """
        res = self.execute_query(query)
        for row in res:
            print("User {} has {} trackpoints in the forbidden city".format(row[0], row[1]))

    # Query 11 - Find all users who have registered transportation_mode and their most used transportation_mode
    # TODO ikke helt ferdig
    def users_registered_transportation_mode_and_their_most_used_transportation_mode(self):
        pass
    

def main():
    query = QueryExecution()
    query.sum_user_activity_trackpoint()
    query.average_number_of_activities_per_user()
    query.top_twenty_users()
    query.users_taken_taxi()
    query.activity_transport_mode_count()
    query.year_with_most_activities()
    query.total_distance_in_km_walked_in_2008_by_userid_112()
    query.top_20_users_gained_most_altitude_meters()
    query.invalid_activities_per_user()
    query.users_tracked_activity_in_the_forbidden_city_beijing()

main()

