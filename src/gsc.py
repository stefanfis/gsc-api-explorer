#!/usr/bin/env python3


USAGE = """Usage:
    ./gsc.py import DIRECTORY
        DIRECTORY       contains monthly directories with daily .csv files ending in '_detailed_web.csv'
        
    ./gsc.py classify [monthly|yearly|all] CLASS_FILE
        CLASS_FILE      a CSV file with two columns: Term,Class, e.g. car,vehicle
        
    ./gsc.py query QUERY
        QUERY           match this query
    """



"""
    
"""


import sys
import time
import datetime
import re
import csv
import json
import os
import os.path
import glob
import sqlite3
from pprint import pprint




class GSC():
    """
    GSC class
    """
    
    DB_NAME = '_gsc.sq3'
    
    
    
    def __init__(self):
        pass


    def get_files(self, my_path):
        """
        Returns a list of files that should get imported
        """
        files = glob.glob(my_path + '/**/*_detailed_web.csv', recursive=True)
        return files
        
        
    def init_db(self):
        """
        Creates the database tables
        """
        db_conn = sqlite3.connect(self.DB_NAME)
        db_cursor = db_conn.cursor()
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS gsc_queries (
                            id INTEGER PRIMARY KEY AUTOINCREMENT, gsc_query TEXT,
                            UNIQUE(gsc_query) ON CONFLICT IGNORE
                        )''')
        db_cursor.execute('''CREATE TABLE IF NOT EXISTS gsc_results (
                           id INTEGER PRIMARY KEY AUTOINCREMENT,
                           gsc_date TEXT,
                           gsc_query INTEGER,
                           gsc_page TEXT,
                           device TEXT,
                           country TEXT,
                           clicks INTEGER,
                           impressions INTEGER,
                           ctr REAL,
                           position REAL,
                           UNIQUE(gsc_date, gsc_query, device, country) ON CONFLICT IGNORE,
                           INDEX(gsc_date)
                    )''')
        db_conn.commit()
        db_conn.close()
        
        
        
    def add_row_to_db(self, db_cursor, row):
        """
        Adds the data in the given row to the database
        """
        db_cursor.execute( "INSERT OR IGNORE INTO gsc_queries (gsc_query) VALUES (?)", (row[1],) )
        db_cursor.execute( "SELECT id FROM gsc_queries WHERE gsc_query = ?", (row[1], ) )
        row[1] = db_cursor.fetchone()[0]
        db_cursor.execute( '''
            INSERT OR IGNORE INTO gsc_results (gsc_date, gsc_query, gsc_page, device, country, clicks, impressions, ctr, position)
            VALUES (?,?,?,?,?,?,?,?,?)''',
            row
        )
        
        
        
    def import_file(self, filename):
        """
        Imports the data from a single CSV file. The first row is omitted.
        """
        with open(filename) as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader, None)                           # skip the header row
            db_conn = sqlite3.connect(self.DB_NAME)
            db_cursor = db_conn.cursor()
            for row in csvreader:
                self.add_row_to_db(db_cursor, row)
            db_conn.commit()
            db_conn.close()


    def import_files(self, file_list):
        """
        Imports all the files given in file_list
        """
        for my_file in file_list:
            print(my_file, file=sys.stderr)
            self.import_file(my_file)


    def read_class_file(self, class_file):
        """
        Reads the term-class definition file (CSV) and returns a dictionary.
        """
        term_class_dict = {}
        with open(class_file) as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader, None)                           # skip the header row
            for row in csvreader:
                my_term = row[0].lower()
                if my_term not in term_class_dict:
                    term_class_dict[my_term] = [row[1]]
                else:
                    term_class_dict[my_term].append(row[1])
        return term_class_dict


    def classify(self, class_file, granularity):
        """
        Classifies the queries in the database according to the given class file
        """
        terms = self.read_class_file(class_file)
        classes = {}
        db_conn = sqlite3.connect(self.DB_NAME)
        db_cursor = db_conn.cursor()
        i = 0
        for term in terms:
            print(term, terms[term], file=sys.stderr)
            if granularity == 'monthly':
                sql = '''
                    SELECT SUBSTR(gsc_date,0,7) AS date_range, SUM(clicks), SUM(impressions)
                    FROM gsc_queries, gsc_results
                    WHERE gsc_queries.id = gsc_results.gsc_query
                        AND gsc_queries.gsc_query LIKE ?
                    GROUP BY date_range ORDER BY date_range ASC
                '''
            elif granularity == 'yearly':
                sql = '''
                    SELECT SUBSTR(gsc_date,0,4) AS date_range, SUM(clicks), SUM(impressions)
                    FROM gsc_queries, gsc_results
                    WHERE gsc_queries.id = gsc_results.gsc_query
                        AND gsc_queries.gsc_query LIKE ?
                    GROUP BY date_range ORDER BY date_range ASC
                '''
            else:
                sql = '''
                    SELECT SUM(clicks), SUM(impressions)
                    FROM gsc_queries, gsc_results
                    WHERE gsc_queries.id = gsc_results.gsc_query
                        AND gsc_queries.gsc_query LIKE ?
                '''
            db_cursor.execute(sql, ('%' + term + '%', ) )
            db_res = db_cursor.fetchall()
            for res in db_res:
                for my_class in terms[term]:
                    if len(res) >= 3:
                        if my_class not in classes:
                            classes[my_class] = {}
                        my_range = res[0]
                        if my_range not in classes[my_class]:
                            classes[my_class][my_range] = { 'clicks': 0, 'impressions': 0}
                        classes[my_class][my_range]['clicks'] += res[1]
                        classes[my_class][my_range]['impressions'] += res[2]
                    else:
                        if my_class not in classes:
                            classes[my_class] = { 'clicks': 0, 'impressions': 0}
                        classes[my_class]['clicks'] += res[0]
                        classes[my_class]['impressions'] += res[1]
            i += 1
            if i >= 3:
                break
        db_conn.close()
        print(json.dumps(classes, sort_keys=True, indent=4))


    def query(self, my_query):
        """
        """
        db_conn = sqlite3.connect(self.DB_NAME)
        db_cursor = db_conn.cursor()
        sql = '''
            SELECT
                gsc_queries.gsc_query,
                SUM(clicks),
                SUM(impressions) AS sum_impressions,
                AVG(position)
            FROM gsc_queries, gsc_results
            WHERE
                gsc_queries.gsc_query LIKE ? AND
                gsc_queries.id = gsc_results.gsc_query
            GROUP BY gsc_results.gsc_query
            ORDER BY sum_impressions DESC
            LIMIT 50
        '''
        db_cursor.execute(sql, (my_query,) )
        db_res = db_cursor.fetchall()
        for res in db_res:
            print(res)
        db_conn.close()
    
    
    
    def get_data_range(self):
        """
        Gets the date of the first and the last saved results
        """
        db_conn = sqlite3.connect(self.DB_NAME)
        db_cursor = db_conn.cursor()
        db_cursor.execute( "SELECT MIN(gsc_date), MAX(gsc_date) FROM gsc_results" )
        date_range = db_cursor.fetchone()
        db_conn.close()
        return date_range




def soundex(s):
    """
    https://stackoverflow.com/a/67197882/2771733
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = s.upper()
    replacements = (
        ("BFPV", "1"),
        ("CGJKQSXZ", "2"),
        ("DT", "3"),
        ("L", "4"),
        ("MN", "5"),
        ("R", "6"),
    )
    result = [s[0]]
    count = 1
    # find would-be replacment for first character
    for lset, sub in replacements:
        if s[0] in lset:
            last = sub
            break
    else:
        last = None
    for letter in s[1:]:
        for lset, sub in replacements:
            if letter in lset:
                if sub != last:
                    result.append(sub)
                    count += 1
                last = sub
                break
        else:
            if letter != "H" and letter != "W":
                last = None
        if count == 4:
            break
    result += "0" * (4 - count)
    return "".join(result)




if __name__ == '__main__':
    
    gsc = GSC()
    my_cmd = sys.argv[1]

    try:
        if my_cmd == 'import':
            my_path = sys.argv[2]
            file_list = gsc.get_files(my_path)
            gsc.init_db()
            gsc.import_files(file_list)
            
        if my_cmd == 'classify':
            granularity = sys.argv[2]
            my_class_file = sys.argv[3]
            print("This may take a while, get some fresh coffee ...", file=sys.stderr)
            gsc.classify(my_class_file, granularity)
            
        if my_cmd == 'query':
            my_query = sys.argv[2]
            gsc.query(my_query)
        
            
    except ValueError:
        print(USAGE, file=sys.stderr)
    
    
    
    
    
    
    
    
    
    