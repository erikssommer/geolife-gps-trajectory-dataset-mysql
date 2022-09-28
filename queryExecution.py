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

    # Query 2

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

    # Query 5
    
    # Query 6

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

