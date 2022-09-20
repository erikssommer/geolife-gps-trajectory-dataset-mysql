#!/bin/bash

echo "✨ Running db_init.sh"

mysql -u root -p$MYSQL_ROOT_PASSWORD -e "
    FLUSH PRIVILEGES;
    CREATE USER '$MYSQL_USER'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD';
    
    CREATE TABLE $MYSQL_DATABASE.User ( 
        id VARCHAR(255) NOT NULL PRIMARY KEY,
        has_labels BOOLEAN
    );

    CREATE TABLE $MYSQL_DATABASE.Activity ( 
        id INT NOT NULL PRIMARY KEY,
        user_id VARCHAR(255) NOT NULL,
        transportation_mode VARCHAR(255),
        start_date_time DATETIME,
        end_date_time DATETIME,
        FOREIGN KEY (user_id) REFERENCES User(id)
    );

    CREATE TABLE $MYSQL_DATABASE.TrackPoint ( 
        id INT NOT NULL PRIMARY KEY,
        activity_id INT NOT NULL,
        lat DOUBLE,
        lon DOUBLE,
        altitude INT,
        date_time DATETIME,
        FOREIGN KEY (activity_id) REFERENCES Activity(id)
    );
"