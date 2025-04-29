from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table
from pyhive import hive

class MongoDBGradeManager:
    def __init__(self, csv_path):
        self.client = MongoClient()
        self.db = self.client.new_database
        self.csv_path = "/home/iiitb/NOSQL_PROJECT/student_course_grades.csv"
        self.initialize_collections()
        
    def initialize_collections(self):
        # Drop and recreate grades collection
        if 'grades' in self.db.list_collection_names():
            self.db.grades.drop()
        self.grades = self.db.grades
        
        # Drop and recreate oplogs collection
        if 'oplogs' in self.db.list_collection_names():
            self.db.oplogs.drop()
        self.oplogs = self.db.oplogs
        
        # Create compound index for composite primary key
        self.grades.create_index(
            [("student-ID", 1), ("course-id", 1)],
            unique=True
        )
        
        # Load CSV data        
        self.load_csv_data()
    
    def load_csv_data(self):
        # Read CSV using pandas
        df = pd.read_csv(self.csv_path)
        
        # Convert to dictionary records        
        records = df.to_dict('records')
        
        # Insert into MongoDB        
        if records:
            self.grades.insert_many(records)
    
    def get(self, student_id, course_id):
        # Find document using composite key
        doc = self.grades.find_one({
            "student-ID": student_id,
            "course-id": course_id
        })
        
        # Log GET operation        
        self._log_operation("GET", student_id, course_id)
        return doc["grade"] if doc else None
    
    def set(self, student_id, course_id, new_grade):
        # Update or insert document    
        self.grades.update_one(
            {"student-ID": student_id, "course-id": course_id},
            {"$set": {"grade": new_grade}},
            upsert=True
        )
        # Log SET operation        
        self._log_operation("SET", student_id, course_id, new_grade)
    
    def _log_operation(self, operation, student_id, course_id, new_grade='X'):
        # Generate timestamp with milliseconds precision
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        
        # Create oplog entry       
        oplog_entry = {
            "timestamp": timestamp,
            "operation": operation,
            "student-id": student_id,
            "course-id": course_id,
            "new-grade": new_grade
        }
        # print("Logging to MongoDB oplogs:", oplog_entry)
        self.oplogs.insert_one(oplog_entry)


# Continuation inside MongoDBGradeManager
    def merge(self, source_system, db_url=None):
        kv_store = {}

        if source_system.lower() == "sql":
            db_url = "postgresql+psycopg2://postgres:SQL%40123@localhost:5432/new_database"
            if db_url is None:
                print("db_url required for SQL source.")
                return
            
            # Connect to SQL
            
            engine = create_engine(db_url)
            metadata = MetaData()
            metadata.reflect(bind=engine)
            
            sql_oplogs = Table('oplogs', metadata, autoload_with=engine)
            
            with engine.connect() as conn:
                #query = sql_oplogs.select().where(sql_oplogs.c.operation == "SET")
                result = conn.execute(text("""
                SELECT log_timestamp as timestamp, 
                       student_id as "student-id", 
                       course_id as "course-id", 
                       new_grade as "new-grade"
                FROM oplogs 
                WHERE operation = 'SET'
                """))

                for row in results:
                    key = (row['student-id'], row['course-id'])
                    ts = row['timestamp']
                    new_grade = row['new_grade']
                    if key not in kv_store or ts > kv_store[key][0]:
                        kv_store[key] = (ts, new_grade)
            
            print(f"Fetched {len(kv_store)} records from SQL.")

        elif source_system.lower() == "hive":
            # Connect to Hive
            hive_conn = hive.Connection(host='127.0.0.1', port=10000, auth='NOSASL', username='iiitb', database='new_database')
            with hive_conn.cursor() as cursor:
                cursor.execute("SELECT timestamp, `student-ID`, `course-id`, new_grade FROM oplogs WHERE operation = 'SET'")
                hive_oplogs = cursor.fetchall()

                for ts, student_id, course_id, new_grade in hive_oplogs:
                    key = (student_id, course_id)
                    if key not in kv_store or ts > kv_store[key][0]:
                        kv_store[key] = (ts, new_grade)
            
            print(f"Fetched {len(kv_store)} records from Hive.")

        else:
            print(f"Merge from {source_system} not supported yet.")
            return
        
        # Step 2: Update MongoDB grades collection based on kv_store
        for (student_id, course_id), (ts, new_grade) in kv_store.items():
            self.set(student_id, course_id, float(new_grade))

        print(f"Merged {len(kv_store)} records into MongoDB from {source_system.upper()}.")

