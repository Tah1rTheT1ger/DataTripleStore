from sqlalchemy import create_engine, Column, String, Float, DateTime, PrimaryKeyConstraint, Table, MetaData, insert, update, text
import pandas as pd
from datetime import datetime
from pyhive import hive
from pymongo import MongoClient

class SQLGradeManager:
    def __init__(self, db_url, csv_path):
        self.engine = create_engine(db_url)
        self.csv_path = csv_path
        self.metadata = MetaData()
        self.initialize_tables()

    def initialize_tables(self):
        # Define grades table
        self.grades = Table(
            'grades', self.metadata,
            Column('student-ID', String, nullable=False),
            Column('course-id', String, nullable=False),
            Column('grade',String, nullable=False),
            PrimaryKeyConstraint('student-ID', 'course-id')
        )
        
        # Define oplogs table
        self.oplogs = Table(
            'oplogs', self.metadata,
            Column('timestamp', String, nullable=False),
            Column('operation', String, nullable=False),
            Column('student-ID', String, nullable=False),
            Column('course-id', String, nullable=False),
            Column('new_grade', String, nullable=False),
        )
        
        # Drop and recreate tables
        self.metadata.drop_all(self.engine)
        self.metadata.create_all(self.engine)
        
        # Load CSV data
        self.load_csv_data()

    def load_csv_data(self):
        df = pd.read_csv(self.csv_path)

        if not df.empty:
            # Rename columns to match the grades table schema
            df = df.rename(columns={
                'student-ID': 'student-ID',
                'course-id': 'course-id',
                'grade': 'grade'
            })

            # Drop unnecessary columns
            df = df.drop(columns=['roll no', 'email ID'], errors='ignore')

            # Keep only the necessary columns
            df = df[['student-ID', 'course-id', 'grade']]

            # Insert into the grades table
            df.to_sql('grades', self.engine, if_exists='append', index=False)

    def get(self, student_id, course_id):
        with self.engine.begin() as conn:
            query = self.grades.select().where(
                (self.grades.c["student-ID"] == student_id) &
                (self.grades.c["course-id"] == course_id)
            )
            result = conn.execute(query).fetchone()
            if result is None:
                print(f"There is no combination of student_id {student_id} and course_id {course_id}")
                return None
            # Log GET operation
            self._log_operation(conn, "GET", student_id, course_id)
            return result[2]

    def set(self, student_id, course_id, new_grade):
        with self.engine.begin() as conn:  # <- this ensures auto-commit
            # Try to update first
            update_stmt = update(self.grades).where(
                (self.grades.c["student-ID"] == student_id) &
                (self.grades.c["course-id"] == course_id)
            ).values(grade=new_grade)
            result = conn.execute(update_stmt)

            if result.rowcount == 0:
                print(f"There is no combination of student_id {student_id} and course_id {course_id}")
                return           
            # Log SET operation
            self._log_operation(conn, "SET", student_id, course_id, new_grade)


    def _log_operation(self, conn, operation, student_id, course_id, new_grade='X'):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        insert_stmt = insert(self.oplogs).values(
            **{
                "timestamp": timestamp,
                "operation": operation,
                "student-ID": student_id,
                "course-id": course_id,
                "new_grade": str(new_grade)
            }
        )
        conn.execute(insert_stmt)

    def _log_operation2(self, conn, operation, student_id, course_id, timestamp ,new_grade):
        # timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        insert_stmt = insert(self.oplogs).values(
            **{
                "timestamp": timestamp,
                "operation": operation,
                "student-ID": student_id,
                "course-id": course_id,
                "new_grade": str(new_grade)
            }
        )
        conn.execute(insert_stmt)    

    def merge(self, source_system):
        kv_store = {}

        if source_system.lower() == "hive":
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
                                    kv_store[key] = (ts, new_grade, "remote")
                                    
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
        else:
            print(f"Merge from {source_system} not supported yet.")
            return
        
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
                    # Store with local flag
                    if key not in kv_store or ts > kv_store[key][0]:
                        kv_store[key] = (ts, row['new_grade'], "local")
            
            print(f"Fetched {len(kv_store)} records from SQL.")
        except Exception as e:
            print(f"Error merging from SQL: {e}")
            return

        # Filter out remote records
        filtered_kv_store = {k: v for k, v in kv_store.items() if v[2] == "remote"}

        # Apply changes to PostgreSQL
        with self.engine.begin() as conn:
            for (student_id, course_id), (ts, new_grade, flag) in filtered_kv_store.items():
                update_stmt = update(self.grades).where(
                    (self.grades.c["student-ID"] == student_id) &
                    (self.grades.c["course-id"] == course_id)
                ).values(grade=new_grade)
                
                result = conn.execute(update_stmt)
                if result.rowcount == 0:
                    insert_stmt = insert(self.grades).values(
                        **{
                            "student-ID": student_id,
                            "course-id": course_id,
                            "grade": new_grade
                        }
                    )
                    conn.execute(insert_stmt)

                self._log_operation2(conn, "SET", student_id, course_id,ts, new_grade)

        print(f"Merged {len(filtered_kv_store)} local records into PostgreSQL from {source_system.upper()}.")