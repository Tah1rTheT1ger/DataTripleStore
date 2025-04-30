from pymongo import MongoClient
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, Table, text
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
        if doc is None:
            print(f"There is no combination of student_id {student_id} and course_id {course_id}")
            return None
        # Log GET operation        
        self._log_operation("GET", student_id, course_id)
        return doc["grade"]
    
    def set(self, student_id, course_id, new_grade):
        # Update or insert document    
        result = self.grades.update_one(
            {"student-ID": student_id, "course-id": course_id},
            {"$set": {"grade": new_grade}},
        )
        if result.matched_count == 0:
            # No existing document found
            print(f"There is no combination of student_id {student_id} and course_id {course_id}")
            return  # Exit without inserting
    
        # Log SET operation        
        self._log_operation("SET", student_id, course_id, new_grade)
       
    def log2(self, operation, student_id, course_id, ts, new_grade):
        # Update or insert document    
        result = self.grades.update_one(
            {"student-ID": student_id, "course-id": course_id},
            {"$set": {"grade": new_grade}},
        )
        if result.matched_count == 0:
            # No existing document found
            print(f"There is no combination of student_id {student_id} and course_id {course_id}")
            return  # Exit without inserting
    
        # Generate timestamp with milliseconds precision
        timestamp = ts
        
        
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
        elif source_system.lower() == "hive":
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
            return
        
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
                        kv_store[key] = (ts, doc['new-grade'], "local")
                
                except KeyError as e:
                    print(f"Missing field in MongoDB document: {e}")
                    continue
                
            print(f"Fetched {len(kv_store)} records from MongoDB.")
        except Exception as e:
            print(f"Error merging from MongoDB: {e}")
            return
        # Filter out remote records
        filtered_kv_store = {k: v for k, v in kv_store.items() if v[2] == "remote"}
                # Debug print the filtered key-value store
        print("\nDEBUG - filtered_kv_store contents:")
        for (student_id, course_id), (ts, grade, flag) in filtered_kv_store.items():
            print(f"  Key: ({student_id}, {course_id}) -> Value: (ts={ts}, grade={grade}, flag={flag})")

        if filtered_kv_store:
            # Apply changes to MongoDB - remove float() conversion
            print("trigerred")
            for (student_id, course_id), (ts, new_grade, flag) in filtered_kv_store.items():

                self.log2("SET", student_id, course_id, ts, new_grade)  # Keep as string

            print(f"Merged {len(filtered_kv_store)} records into MongoDB from {source_system.upper()}.")
    
        # elif source_system.lower() == "hive":
        #     try:
        #         # Establish Hive connection with simpler configuration
        #         hive_conn = hive.Connection(
        #             host='127.0.0.1',
        #             port=10000,
        #             username='iiitb',
        #             database='new_database',
        #         )
                
        #         with hive_conn.cursor() as cursor:
        #             try:
        #                 # Use the exact column names as they appear in Hive
        #                 cursor.execute("""
        #                     SELECT 
        #                         log_timestamp,
        #                         `student-id`, 
        #                         `course-id`, 
        #                         new_grade
        #                     FROM oplogs 
        #                     WHERE operation = 'SET'
        #                 """)
                        
        #                 for row in cursor.fetchall():
        #                     try:
        #                         ts, student_id, course_id, new_grade = row
        #                         key = (student_id, course_id)
                                
        #                         # Parse timestamp
        #                         if isinstance(ts, str):
        #                             try:
        #                                 ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S.%f')
        #                             except ValueError:
        #                                 ts = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                                
        #                         # Store only if newer than existing record
        #                         if key not in kv_store or ts > kv_store[key][0]:
        #                             kv_store[key] = (ts, new_grade)
                                    
        #                     except Exception as e:
        #                         print(f"Error processing row {row}: {e}")
        #                         continue
                        
        #                 print(f"Fetched {len(kv_store)} records from Hive.")
                        
        #             except Exception as e:
        #                 print(f"Error executing Hive query: {e}")
        #                 return
                    
        #     except Exception as e:
        #         print(f"Error connecting to Hive: {e}")
        #         return

        # # Apply changes to MongoDB - remove float() conversion
        # for (student_id, course_id), (ts, new_grade) in kv_store.items():
        #     self.set(student_id, course_id, new_grade)  # Keep as string

        # print(f"Merged {len(kv_store)} records into MongoDB from {source_system.upper()}.")