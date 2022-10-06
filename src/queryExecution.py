from dis import dis
from turtle import distance
from DbConnector import DbConnector
from haversine import haversine

class QueryExecution:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def sum_user_activity_trackpoint(self):
        """
        Query 1 - Finding how many users, activities and trackpoints are there in the dataset
        """

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
        print("There are {} users, {:,} activities and {:,} trackpoints in the dataset".format(
            user_sum, activity_sum, trackpoint_sum).replace(",", " "))

    def average_number_of_activities_per_user(self):
        """
        Query 2 - Find the average number of activities per user.
        """

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
        print("The average number of activities per user is {:.2f}".format(average))

    def top_twenty_users(self):
        """
        Query 3 - Find the top 20 users with the highest number of activities.
        """

        query = """
            SELECT user_id, COUNT(*) AS num_activities 
            FROM Activity
            GROUP BY user_id
            ORDER BY num_activities DESC
            LIMIT 20
        """
        res = self.execute_query(query)

        
        print("nr. user_id activities\n")
        for i, user in enumerate(res):
            print("{:2} {:>8} {:>10}".format(i + 1, user[0], user[1]))

    def users_taken_taxi(self):
        """
        Query 4 - Find all users who have taken a taxi.
        """

        query = """
            SELECT DISTINCT user_id
            FROM Activity
            WHERE transportation_mode = "taxi"
        """
        res = self.execute_query(query)

        print("Users who have taken a taxi: " + ", ".join([x[0] for x in res]))

    def activity_transport_mode_count(self):
        """
        Query 5 - Find all types of transportation modes and count how many activities 
        that are tagged with these transportation mode labels. 
        Does not count the rows where the mode is null.
        """

        query = """
            SELECT transportation_mode, COUNT(transportation_mode) as count
            FROM geolife.Activity
            WHERE transportation_mode != ""
            GROUP BY transportation_mode
            ORDER BY transportation_mode
        """
        res = self.execute_query(query)

        print("mode        count\n")
        for row in res:
            print("{:11} {:>5}".format(row[0], row[1]))

    def year_with_most_activities(self):
        """
        Query 6 - Find the year with the most activities. Testing if this also is the year with most recorded hours
        """
        # Query a - Find the year with the most activities.
        query6a = """
            SELECT YEAR(start_date_time) AS year, COUNT(id) AS count
            FROM Activity
            GROUP BY year
            LIMIT 1
        """
        res6a = self.execute_query(query6a)
        print("The year {} has the most activities with {:,} activities".format(
            res6a[0][0], res6a[0][1]).replace(",", " "))

        # Query b - Testing if this also is the year with most recorded hours
        query6b = """
            SELECT
                YEAR(start_date_time) AS year,
                ROUND(SUM(TIME_TO_SEC(TIMEDIFF(end_date_time, start_date_time))) / 60 / 60) AS duration
            FROM Activity
            GROUP BY year
            LIMIT 5
        """
        res6b = self.execute_query(query6b)
        print("The year {} has the most recorded hours with {:,} hours".format(
            res6b[0][0], res6b[0][1]).replace(",", " "))

        print("\nyear   hours\n")
        for row in res6b:
            print("{}  {:>6,}".format(row[0], row[1]).replace(",", " "))

        # Testing if the year with most activities also is the year with most recorded hours
        if res6b[0][0] == res6a[0][0]:
            print("\nYes, this is also the year with most recorded hours!")
        else:
            print("\nNo, this is not the year with most recorded hours")

    def total_distance_in_km_walked_in_2008_by_userid_112(self):
        """ 
        Query 7 - Find the total distance (in km) walked in 2008, by user with id = 112
        """
        # Finding longitude and latitude for each trackpoint matching the user_id, mode and year
        query = """
            SELECT lat, lon
            FROM TrackPoint
            WHERE activity_id
            IN (
                SELECT id FROM Activity
                WHERE user_id = 112 
                AND transportation_mode = 'walk' 
                AND YEAR(start_date_time) = 2008
            )
        """

        res = self.execute_query(query)
        distance = 0
        for i in range(len(res)):
            if i == len(res)-1:
                break
            # Calculating distance between two points using haversine formula
            distance += haversine(res[i], res[i+1])
            
        print("The total distance walked in 2008 by user 112 is {:.2f} km".format(distance))

    def top_20_users_gained_most_altitude_meters(self):
        """
        Query 8 - Find the top 20 users who have gained the most altitude meters.
        """

        query = """
            SELECT 
                user_id,
                activity_id,
                TrackPoint.altitude
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
        """

        trackpoint_altitudes = self.execute_query(query)
        user_altitude = dict()
        
        # Calculating the altitude gained for each user
        for index in range(len(trackpoint_altitudes)):
            # If the user is not in the dictionary, add it
            if index == len(trackpoint_altitudes) - 1:
                break
            user_id = trackpoint_altitudes[index][0]
            activity_id = trackpoint_altitudes[index][1]

            next_activity_id = trackpoint_altitudes[index + 1][1]

            # We can only calculate the altitude gain if we have two trackpoints from the same activity
            if activity_id != next_activity_id:
                continue

            # Initialize the user_altitude dict if the user_id is not in it
            if user_id not in user_altitude:
                user_altitude[user_id] = 0

            altitude = trackpoint_altitudes[index][2]
            next_altitude = trackpoint_altitudes[index + 1][2]
        
            # If one of the altitudes are null they were -777 before cleanup and are invalid
            if not altitude or not next_altitude:
                continue
                
            altitude_diff = next_altitude - altitude
            user_altitude[user_id] += altitude_diff

        user_altitude_array = sorted(user_altitude.items(), key=lambda x: x[1], reverse=True)

        print("nr. user_id altitude\n")
        for i, (user_id, altitude) in enumerate(user_altitude_array[:20]):
            print("{:3} {:>7} {:>8}".format(i + 1, user_id, altitude))

    def invalid_activities_per_user(self):
        """
        Query 9 - Find all users who have invalid activities, and the number of invalid activities per user
        """
        
        query = """
            SELECT user_id, COUNT(DISTINCT activity_id) AS fault_activity_amount
            FROM(
                SELECT MINUTE(TIMEDIFF(startTime, prev_time)) AS time_diff, user_id, activity_id, prev_a_id
                    FROM(
                        SELECT t1.date_time AS startTime,
                            LAG(t1.date_time) OVER(ORDER BY date_time) AS prev_time,
                            user_id, activity_id,
                            LAG(t1.activity_id) OVER(ORDER BY date_time) AS prev_a_id
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
        print("user_id  invalid_activities\n")
        for row in res:
            print("{} {:>23}".format(row[0], row[1]))

    def users_tracked_activity_in_the_forbidden_city_beijing(self):
        """
        Query 10 - Find the users who have tracked an activity in the Forbidden City of Beijing.
        """

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
            print("User {} has {} trackpoints in the forbidden city".format(
                row[0], row[1]))

    def users_registered_transportation_mode_and_their_most_used_transportation_mode(self):
        """
        Query 11 - Find all users who have registered transportation_mode and their most used transportation_mode
        """
        
        query = """
            WITH top_modes_per_user as (
                SELECT user_id, transportation_mode, COUNT(Activity.id) as count
                FROM Activity
                JOIN User ON Activity.user_id = `User`.id
                WHERE transportation_mode != ""
                GROUP BY user_id, transportation_mode
            )
            SELECT tm.*
            FROM top_modes_per_user tm
            INNER JOIN
                (SELECT user_id, MAX(count) AS max_count
                FROM top_modes_per_user
                GROUP BY user_id) groupedtm 
            ON tm.user_id = groupedtm.user_id 
            AND tm.count = groupedtm.max_count
        """
        res = self.execute_query(query)
        if len(res) == 0:
            print("No users have registered transportation mode")

        print("user_id transportation_mode count\n")
        for user in res:
            print("{:7} {:18} {:6}".format(user[0], user[1], user[2]))
