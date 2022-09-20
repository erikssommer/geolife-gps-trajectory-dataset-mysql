from DbConnector import DbConnector
from tabulate import tabulate


class CreateTables:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    @staticmethod
    def user_table_query():
        query = """CREATE TABLE IF NOT EXISTS User (
                    id VARCHAR(255) NOT NULL PRIMARY KEY,
                    has_labels BOOLEAN
                )
                """
        return query

    @staticmethod
    def activity_table_query():
        query = """CREATE TABLE IF NOT EXISTS Activity (
                    id INT NOT NULL PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    transportation_mode VARCHAR(255),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES User(id)
                )
                """
        return query
    
    @staticmethod
    def trackpoint_table_query():
        query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                    id INT NOT NULL PRIMARY KEY,
                    activity_id INT NOT NULL,
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INT,
                    date_time DATETIME,
                    FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
        return query

    def create_table(self, table_name):
        query = ""
        if table_name == "User":
            query = CreateTables.user_table_query()
        elif table_name == "Activity":
            query = CreateTables.activity_table_query()
        elif table_name == "TrackPoint":
            query = CreateTables.create_trackpoint_table()
        else:
            raise Exception("Invalid table name")

        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    """
    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name) 
    
    """


def main():
    program = None
    try:
        program = CreateTables()
        program.create_table(table_name="User")
        program.create_table(table_name="Activity")
        program.create_table(table_name="TrackPoint")
        # Check that the table is dropped
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
