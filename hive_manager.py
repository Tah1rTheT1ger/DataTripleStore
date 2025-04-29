from pyhive import hive
from datetime import datetime
import pandas as pd
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
        self._log_operation('GET', student_id, course_id)
        return result[0][0] if result else None

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


    def merge(self, source_system):
        kv_store = {}

        if source_system.lower() == "sql":
            db_url = "postgresql+psycopg2://postgres:yourpassword@localhost:5432/new_database"
            engine = create_engine(db_url)
            with engine.connect() as conn:
                result = conn.execute(text("SELECT timestamp, student_id, course_id, new_grade FROM oplogs WHERE operation = 'SET'"))
                for row in result:
                    key = (row.student_id, row.course_id)
                    ts = row.timestamp
                    if key not in kv_store or ts > kv_store[key][0]:
                        kv_store[key] = (ts, row.new_grade)
            print(f"Fetched {len(kv_store)} records from SQL.")

        elif source_system.lower() == "hive":
            result = self.execute('''
                SELECT timestamp, `student-ID`, `course-id`, new_grade 
                FROM new_database.oplogs 
                WHERE operation = 'SET'
            ''')
            for ts, student_id, course_id, new_grade in result:
                key = (student_id, course_id)
                if key not in kv_store or ts > kv_store[key][0]:
                    kv_store[key] = (ts, new_grade)
            print(f"Fetched {len(kv_store)} records from Hive.")

        else:
            print(f"Merge from {source_system} not supported yet.")
            return

        for (student_id, course_id), (ts, new_grade) in kv_store.items():
            self.set(student_id, course_id, new_grade)

        print(f"Merged {len(kv_store)} records into Hive from {source_system.upper()}.")
