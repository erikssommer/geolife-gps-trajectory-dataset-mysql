#!/bin/bash

echo "✨ Running db_init.sh"

mysql -u root -p$MYSQL_ROOT_PASSWORD -e "
    FLUSH PRIVILEGES;
    CREATE USER '$MYSQL_USER'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD';
    CREATE TABLE $MYSQL_DATABASE.user (id int AUTO_INCREMENT, PRIMARY KEY (id));
"