import DbConnector

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
        return [
            self.execute_query("SELECT COUNT(*) FROM geolife.`User`"),
            self.execute_query("SELECT COUNT(*) FROM geolife.Activity"),
            self.execute_query("SELECT COUNT(*) FROM geolife.TrackPoint")
        ]

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
        self.execute_query(query)

    # Query 3
    def top_twenty_users(self):
        query = """
            SELECT user_id, COUNT(*) AS num_activities 
            FROM Activity
            GROUP BY user_id
            ORDER BY num_activities
            DESC LIMIT 20
        """
        self.execute_query(query)

    # Query 4
    def query4(self):
        query = """
            SELECT transportation_mode, COUNT(transportation_mode) as count
            FROM geolife.Activity
            WHERE transportation_mode!=""
            GROUP BY transportation_mode
        """
        self.execute_query(query)

    # Query 5
    
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

    # Query 8

    # Query 9

    # Query 10

    # Query 11

    # Query 12

def main():
    query = QueryExecution()
    query.top_twenty_users()

if __name__ == "__main__":
    main()
