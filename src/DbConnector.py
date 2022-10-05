import mysql.connector as mysql
from decouple import config

class DbConnector:
    """
    Connects to the MySQL server on docker.
    Connector needs HOST, DATABASE, USER and PASSWORD to connect,
    while PORT is optional and should be 3306.

    Example:
    HOST = "localhost" // Your server IP address/domain name
    DATABASE = "testdb" // Database name, if you just want to connect to MySQL server, leave it empty
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    def __init__(self,
                 HOST=config('MYSQL_DATABASE_HOST'),
                 PORT=config('MYSQL_DATABASE_PORT'),
                 DATABASE=config('MYSQL_DATABASE'),
                 USER=config('MYSQL_USER'),
                 PASSWORD=config('MYSQL_PASSWORD')):
        # Connect to the database
        try:
            self.db_connection = mysql.connect(host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=PORT)
        except Exception as e:
            print("ERROR: Failed to connect to db:", e)

        # Get the db cursor
        self.cursor = self.db_connection.cursor()

        print("Connected to:", self.db_connection.get_server_info())
        # get database information
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        # close the cursor
        self.cursor.close()
        # close the DB connection
        self.db_connection.close()
        print("\n-----------------------------------------------")
        print("Connection to %s is closed" % self.db_connection.get_server_info())
