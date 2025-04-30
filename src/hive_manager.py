from pyhive import hive
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from sqlalchemy import create_engine, text

class HiveGradeManager:
    def __init__(self, csv_path):
        self.conn = hive.Connection(host='127.0.0.1', port=10000, username='iiitb', database='default')
        self.csv_path = "/home/iiitb/NOSQL_PROJECT/student_course_grades.csv"
        self.initialize_tables()

    def execute(self, query):
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            query_type = query.strip().split()[0].lower()
            if query_type == "select":
                return cursor.fetchall()
            return None

    def initialize_tables(self):
        # Create database if not exists
        self.execute('CREATE DATABASE IF NOT EXISTS new_database')

        # Drop existing grades table and create a new non-ACID table
        self.execute('DROP TABLE IF EXISTS new_database.grades')
        self.execute('''
            CREATE TABLE new_database.grades (
                `student-ID` STRING,
                `course-id` STRING,
                `roll_no` STRING,
                `email_ID` STRING,
                `grade` STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        ''')
        self.execute('DROP TABLE IF EXISTS new_database.oplogs')
        self.execute('''
            CREATE TABLE new_database.oplogs (
                log_timestamp STRING,
                operation STRING,
                `student-ID` STRING,
                `course-id` STRING,
                new_grade STRING
            )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        ''')


        # Load CSV data into grades table
        self.execute(f'''
            LOAD DATA LOCAL INPATH '{self.csv_path}'
            OVERWRITE INTO TABLE new_database.grades
        ''')
    def _log_operation(self, operation, student_id, course_id, new_grade='X'):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        # Log operation to the oplogs table
        self.execute(f'''
            INSERT INTO TABLE new_database.oplogs
            VALUES (
                '{timestamp}',
                '{operation}',
                '{student_id}',
                '{course_id}',
                '{new_grade}'
            )
        ''')


    def get(self, student_id, course_id):
        result = self.execute(f'''
            SELECT grade FROM new_database.grades
            WHERE `student-ID` = '{student_id}'
              AND `course-id` = '{course_id}'
        ''')
        if not result:
            print(f"No combination of student_id '{student_id}' and course_id '{course_id}' exists")
            return None
        self._log_operation('GET', student_id, course_id)
        return result[0][0]

    def set(self, student_id, course_id, new_grade):
        """Update the grade in Hive for the given student_id and course_id."""
        try:
            cursor = self.conn.cursor()

            # Step 1: Overwrite the table with updated grade
            query = f"""
            INSERT OVERWRITE TABLE new_database.grades
            SELECT `student-ID`, `course-id`, roll_no, email_ID, 
                CASE WHEN `student-ID` = '{student_id}' AND `course-id` = '{course_id}' 
                        THEN '{new_grade}' 
                        ELSE grade 
                END as grade
            FROM new_database.grades
            """
            cursor.execute(query)
            # exists = cursor.fetchone()[0]

            # if exists == 0:
            #     print(f"No combination of student_id '{student_id}' and course_id '{course_id}' exists in Hive")
            #     return False

            # Step 2: Verify the update
            # verify_query = f"""
            # SELECT COUNT(*) 
            # FROM new_database.grades 
            # WHERE `student-ID` = '{student_id}' AND `course-id` = '{course_id}' AND grade = '{new_grade}'
            # """
            # cursor.execute(verify_query)
            # updated = cursor.fetchone()[0]

            # if updated > 0:
            #     print(f"Grade updated to {new_grade} in Hive for student-ID: {student_id}, course-id: {course_id}")
            # else:
            #     print(f"No record found in Hive for student-ID: {student_id}, course-id: {course_id}")

            # Step 3: Log the operation
            self._log_operation('SET', student_id, course_id, new_grade)
            #return updated > 0

        except Exception as e:
            print(f"Hive Error during SET: {e}")
            return False
        finally:
            if cursor:
                cursor.close()

    def log2(self, operation, student_id, course_id, ts, new_grade):
        """Update the grade in Hive for the given student_id and course_id."""
        try:
            cursor = self.conn.cursor()

            # Step 1: Overwrite the table with updated grade
            query = f"""
            INSERT OVERWRITE TABLE new_database.grades
            SELECT `student-ID`, `course-id`, roll_no, email_ID, 
                CASE WHEN `student-ID` = '{student_id}' AND `course-id` = '{course_id}' 
                        THEN '{new_grade}' 
                        ELSE grade 
                END as grade
            FROM new_database.grades
            """
            cursor.execute(query)
            exists = cursor.fetchone()[0]

            if exists == 0:
                print(f"No combination of student_id '{student_id}' and course_id '{course_id}' exists in Hive")
                return False

            # Step 2: Verify the update
            # verify_query = f"""
            # SELECT COUNT(*) 
            # FROM new_database.grades 
            # WHERE `student-ID` = '{student_id}' AND `course-id` = '{course_id}' AND grade = '{new_grade}'
            # """
            # cursor.execute(verify_query)
            # updated = cursor.fetchone()[0]

            # if updated > 0:
            #     print(f"Grade updated to {new_grade} in Hive for student-ID: {student_id}, course-id: {course_id}")
            # else:
            #     print(f"No record found in Hive for student-ID: {student_id}, course-id: {course_id}")

            # Step 3: Log the operation
            timestamp = ts
        
        # Log operation to the oplogs table
            self.execute(f'''
                INSERT INTO TABLE new_database.oplogs
                VALUES (
                    '{timestamp}',
                    '{operation}',
                    '{student_id}',
                    '{course_id}',
                    '{new_grade}'
                )
            ''')
            #return updated > 0

        except Exception as e:
            print(f"Hive Error during SET: {e}")
            return False
        finally:
            if cursor:
                cursor.close()

    def merge(self, source_system):
        kv_store = {}
        if source_system.lower() == "sql":
            db_url = "postgresql+psycopg2://postgres:SQL%40123@localhost:5432/new_database"
            if db_url is None:
                print("db_url required for SQL source.")
                return
            
            try:
                engine = create_engine(db_url)
                with engine.connect() as conn:
                    query = text("""
                        SELECT timestamp as timestamp,
                            "student-ID" as student_id,
                            "course-id" as course_id,
                            new_grade as new_grade
                        FROM oplogs
                        WHERE operation = 'SET'
                    """)
                    result = conn.execute(query)
                    
                    for row in result.mappings():
                        key = (row['student_id'], row['course_id'])
                        ts = row['timestamp']
                        if isinstance(ts, str):
                            ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
                        if key not in kv_store or ts > kv_store[key][0]:
                            kv_store[key] = (ts, row['new_grade'],"remote")
                
                print(f"Fetched {len(kv_store)} records from SQL.")

            except Exception as e:
                print(f"Error merging from SQL: {e}")
                return
        
        elif source_system.lower() == "mongo":
            try:
                client = MongoClient('localhost', 27017)
                db = client.new_database
                
                oplogs = db.oplogs.find({"operation": "SET"})
                
                for doc in oplogs:
                    try:
                        key = (doc['student-id'], doc['course-id'])
                        ts = doc['timestamp']
                        
                        if isinstance(ts, str):
                            try:
                                ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                        
                        # Store with remote flag
                        if key not in kv_store or ts > kv_store[key][0]:
                            kv_store[key] = (ts, doc['new-grade'], "remote")
                    
                    except KeyError as e:
                        print(f"Missing field in MongoDB document: {e}")
                        continue
                    
                print(f"Fetched {len(kv_store)} records from MongoDB.")

            except Exception as e:
                print(f"Error merging from MongoDB: {e}")
                return
        elif source_system.lower() == "hive":
            return
        try:
            hive_conn = hive.Connection(
                host='127.0.0.1',
                port=10000,
                username='iiitb',
                database='new_database'
            )
            
            with hive_conn.cursor() as cursor:
                try:
                    cursor.execute("""
                        SELECT 
                            log_timestamp,
                            `student-id`, 
                            `course-id`, 
                            new_grade
                        FROM oplogs 
                        WHERE operation = 'SET'
                    """)
                    
                    for ts, student_id, course_id, new_grade in cursor.fetchall():
                        try:
                            key = (student_id, course_id)
                            
                            if isinstance(ts, str):
                                try:
                                    ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
                                except ValueError:
                                    ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                            
                            # Store with remote flag
                            if key not in kv_store or ts > kv_store[key][0]:
                                kv_store[key] = (ts, new_grade, "local")
                                
                        except Exception as e:
                            print(f"Error processing row: {e}")
                            continue
                    
                    print(f"Fetched {len(kv_store)} records from Hive.")
                    
                except Exception as e:
                    print(f"Error executing Hive query: {e}")
                    return
                
        except Exception as e:
            print(f"Error connecting to Hive: {e}")
            return
    
        # Filter out remote records
        filtered_kv_store = {k: v for k, v in kv_store.items() if v[2] == "remote"}
        if(filtered_kv_store):
            # Apply changes to Hive
            for (student_id, course_id), (ts, new_grade, flag) in filtered_kv_store.items():
                self.log2("SET", student_id, course_id, ts, new_grade)

            print(f"Merged {len(filtered_kv_store)} records into Hive from {source_system.upper()}.")
            # if source_system.lower() == "sql":
        #     try:
        #         db_url = "postgresql+psycopg2://postgres:SQL%40123@localhost:5432/new_database"
        #         engine = create_engine(db_url)
        #         with engine.connect() as conn:
        #             # Use the correct column names with hyphens and proper quoting
        #             result = conn.execute(text("""
        #                 SELECT timestamp, "student-ID" as student_id, 
        #                     "course-id" as course_id, new_grade
        #                 FROM oplogs
        #                 WHERE operation = 'SET'
        #             """)).mappings()
                    
        #             for row in result:
        #                 key = (row['student_id'], row['course_id'])
        #                 ts = row['timestamp']
        #                 if isinstance(ts, str):
        #                     ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
        #                 if key not in kv_store or ts > kv_store[key][0]:
        #                     kv_store[key] = (ts, row['new_grade'])
                
        #         print(f"Fetched {len(kv_store)} records from SQL.")

        #     except Exception as e:
        #         print(f"Error merging from SQL: {e}")
        #         return

        # elif source_system.lower() == "mongo":
        #     try:
        #         client = MongoClient('localhost', 27017)
        #         db = client.new_database
        #         oplogs = db.oplogs.find({"operation": "SET"})
                
        #         for doc in oplogs:
        #             key = (doc['student-id'], doc['course-id'])
        #             ts = doc['timestamp']
        #             if isinstance(ts, str):
        #                 ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
        #             if key not in kv_store or ts > kv_store[key][0]:
        #                 kv_store[key] = (ts, doc['new-grade'])
                
        #         print(f"Fetched {len(kv_store)} records from MongoDB.")

        #     except Exception as e:
        #         print(f"Error merging from MongoDB: {e}")
        #         return

        # else:
        #     print(f"Merge from {source_system} not supported yet.")
        #     return

        # Apply changes to Hive
        # for (student_id, course_id), (ts, new_grade) in kv_store.items():
        #     self.set(student_id, course_id, new_grade)

        # print(f"Merged {len(kv_store)} records into Hive from {source_system.upper()}.")