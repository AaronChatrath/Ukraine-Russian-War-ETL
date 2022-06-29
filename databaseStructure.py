from cgi import test
from unicodedata import name
import mysql.connector
from mysql.connector import errorcode
import sys
import pandas as pd

initialisionconfig = {
    'host': 'localhost',
    'user': 'root',
    'passwd': 'your-password'
}

config = {
    'host': 'localhost',
    'user': 'root',
    'passwd': 'your-password',
    'database': 'RUSUKRWAR'
}

def create_database(database_name):
    '''
    Initialising our connection, creating db from passed argument.
    Throwing an error if neccessary and always closing connection.
    '''

    cnx = None
    try:
        cnx = mysql.connector.connect(**initialisionconfig)
        mycursor = cnx.cursor() 
        cmd = "CREATE DATABASE " + database_name
        mycursor.execute(cmd)
    except mysql.connector.Error as e:
        print("Error %d %s" % (e.args[0], e.args[1]))
        sys.exit(1)
    finally:
        if cnx:
            cnx.close()


def create_tables():
    TABLES = {}

    TABLES['personnel_losses'] = (
    "CREATE TABLE `personnel_losses` ("
    "  `pers_loss_ID` int(11) NOT NULL AUTO_INCREMENT,"
    "  `date` date NOT NULL,"
    "  `personnel_per_day` INT NOT NULL,"
    "  PRIMARY KEY (`pers_loss_ID`)"
    ") ENGINE=InnoDB")

    TABLES['equipment_losses'] = (
    "CREATE TABLE `equipment_losses` ("
    "  `eq_loss_ID` int(11) NOT NULL AUTO_INCREMENT,"
    "  `date` date NOT NULL,"
    "  `aircraft_per_day` INT NOT NULL,"
    "  `drone_per_day` INT NOT NULL,"
    "  `helicopter_per_day` INT NULL,"
    "  `tank_per_day` INT NULL,"
    "  `apc_per_day` INT NULL,"
    "  PRIMARY KEY (`eq_loss_ID`)"
    ") ENGINE=InnoDB")

    TABLES['cumulative_losses'] = (
    "CREATE TABLE `cumulative_losses` ("
    "  `cumulative_loss_ID` int(11) NOT NULL AUTO_INCREMENT,"
    "  `date` date NOT NULL,"
    "  `personnel` INT NOT NULL,"
    "  `aircraft` INT NOT NULL,"
    "  `drone` INT NOT NULL,"
    "  `helicopter` INT NOT NULL,"
    "  `tank` INT NOT NULL,"
    "  `apc` INT NOT NULL,"
    "  PRIMARY KEY (`cumulative_loss_ID`)"
    ") ENGINE=InnoDB")

    cnx = None
    cnx = mysql.connector.connect(**config)
    mycursor = cnx.cursor() 

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            mycursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")
    mycursor.close()
    cnx.close()

def insert_data(pers_per_day_df, eq_per_day_df, cumulative_df):
    cnx = None
    add_pers_per_day_data = ("INSERT INTO personnel_losses (date, personnel_per_day) VALUES (%s,%s)")
    add_eq_per_day_data = ("INSERT INTO equipment_losses (date, aircraft_per_day, drone_per_day, helicopter_per_day, tank_per_day, apc_per_day) VALUES (%s,%s,%s,%s,%s,%s)")
    add_cumulative_data = ("INSERT INTO cumulative_losses (date, personnel, aircraft, drone, helicopter, tank, apc) VALUES (%s,%s,%s,%s,%s,%s,%s)")

    try:
            cnx = mysql.connector.connect(**config)
            mycursor = cnx.cursor() 

            for row in pers_per_day_df.rdd.collect():
                mycursor.execute(add_pers_per_day_data, list(row))
            for row in eq_per_day_df.rdd.collect():
                mycursor.execute(add_eq_per_day_data, list(row))
            for row in cumulative_df.rdd.collect():
                mycursor.execute(add_cumulative_data, list(row))
            cnx.commit()
    except mysql.connector.Error as e:
            print("Error %d %s" % (e.args[0], e.args[1]))
            sys.exit(1)
    finally:
            if cnx:
                cnx.close()    