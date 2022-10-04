from DbConnector import DbConnector

class QueryExecution:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    # Query 1
    def query1(self):
        # How many users, activities and trackpoints are there in the dataset
        query = """
            SELECT 
            (SELECT COUNT(id) FROM User) AS user_sum,
            (SELECT COUNT(id) FROM Activity) AS activity_sum,
            (SELECT COUNT(id) FROM TrackPoint) AS trackpoint_sum
        """
        res = self.execute_query(query)
        print(res)

    # Query 2
    def query2(self):
        query = """
            SELECT AVG(count) AS average_number_of_activities
            FROM (
                SELECT user_id, COUNT(*) AS count
                FROM geolife.Activity
                GROUP BY user_id
            ) AS user_activity_count
        """
        res = self.execute_query(query)
        print(res)

    # Query 3
    def top_twenty_users(self):
        query = """
        SELECT user_id, COUNT(*) AS num_activities 
        FROM Activity
        GROUP BY user_id
        ORDER BY num_activities DESC
        LIMIT 20
        """
        res = self.execute_query(query)
        print(res[0])

    # Query 4
    def users_taken_taxi(self):
        query = """
        SELECT DISTINCT user_id
        FROM Activity
        WHERE transportation_mode = "taxi"
        """
        self.execute_query(query)

    # Query 5
    def query5(self):
        query = """
            SELECT transportation_mode, COUNT(transportation_mode) as count
            FROM geolife.Activity
            WHERE transportation_mode!=""
            GROUP BY transportation_mode
        """
        self.execute_query(query)
    
    # Query 6
    def query6a(self):
        query = """
            SELECT YEAR(start_date_time) AS year, COUNT(id) AS count
            FROM Activity
            GROUP BY year
            LIMIT 1
        """
        self.execute_query(query)

    def query6b(self):
        query = """
            SELECT YEAR(start_date_time) AS year, SEC_TO_TIME(SUM(TIME_TO_SEC(TIMEDIFF(end_date_time, start_date_time)))) AS duration
            FROM Activity
            GROUP BY year
            LIMIT 1
        """
        self.execute_query(query)

    # Query 7
    # TODO: This query is not working
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
        print(res)

    # Query 8
    def query8(self):
        query = """
            SELECT Activity.user_id, SUM(TrackPoint.altitude) as "total meters gained per user"
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
            GROUP BY Activity.user_id
            ORDER BY SUM(TrackPoint.altitude) DESC
            LIMIT 20
        """
        self.execute_query(query)

    # Query 9

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
            ORDER BY user_id ASC"""

        self.execute_query(query)


    # Query 10
    def query10(self):
        query = """
            SELECT user_id, COUNT(TrackPoint.id) as 'trackpoints in forbidden city'
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
            WHERE lat >= 39.916 AND lat < 39.917
            AND lon >= 116.397 AND lon < 116.398
            GROUP BY Activity.user_id
        """
        self.execute_query(query)

    # Query 11 # TODO ikke helt ferdig
    def query10(self):
        query = """
            SELECT user_id, COUNT(TrackPoint.id) as 'trackpoints in forbidden city'
            FROM TrackPoint
            JOIN Activity ON TrackPoint.activity_id = Activity.id
            WHERE lat >= 39.916 AND lat < 39.917
            AND lon >= 116.397 AND lon < 116.398
            GROUP BY Activity.user_id
        """
        self.execute_query(query)
    

def main():
    query = QueryExecution()
    query.query1()
    query.query2()
    query.top_twenty_users()
    query.total_distance_in_km_walked_in_2008_by_userid_112()

main()

