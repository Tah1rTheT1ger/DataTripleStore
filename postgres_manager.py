from sqlalchemy import create_engine, Column, String, Float, DateTime, PrimaryKeyConstraint, Table, MetaData, insert, update
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

            # Log GET operation
            self._log_operation(conn, "GET", student_id, course_id)
            return result[2] if result else None

    def set(self, student_id, course_id, new_grade):
        with self.engine.begin() as conn:  # <- this ensures auto-commit
            # Try to update first
            update_stmt = update(self.grades).where(
                (self.grades.c["student-ID"] == student_id) &
                (self.grades.c["course-id"] == course_id)
            ).values(grade=new_grade)
            result = conn.execute(update_stmt)

            if result.rowcount == 0:
                # Insert if not exists
                insert_stmt = insert(self.grades).values(
                    **{
                        "student-ID": student_id,
                        "course-id": course_id,
                        "grade": new_grade
                    }
                )
                conn.execute(insert_stmt)

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

    def merge(self, source_system):
        kv_store = {}

        if source_system.lower() == "hive":
            # Connect to Hive
            hive_conn = hive.Connection(host='127.0.0.1', port=10000, auth='NOSASL', username='iiitb', database='new_database')
            
            with hive_conn.cursor() as cursor:
                # Step 1: Read oplogs from Hive where operation is SET
                cursor.execute("SELECT timestamp, `student-ID`, `course-id`, new_grade FROM oplogs WHERE operation = 'SET'")
                hive_oplogs = cursor.fetchall()

                for ts, student_id, course_id, new_grade in hive_oplogs:
                    key = (student_id, course_id)
                    if key not in kv_store or ts > kv_store[key][0]:
                        kv_store[key] = (ts, new_grade)
            
            print(f"Fetched {len(kv_store)} records from Hive.")

        elif source_system.lower() == "mongo":
            # Connect to MongoDB
            mongo_client = MongoClient()
            mongo_db = mongo_client.new_database
            mongo_oplogs = mongo_db.oplogs

            # Step 1: Read oplogs from MongoDB where operation is SET
            for oplog in mongo_oplogs.find({"operation": "SET"}):
                key = (oplog["student-id"], oplog["course-id"])
                ts = oplog["timestamp"]
                new_grade = oplog["new_grade"]
                
                if key not in kv_store or ts > kv_store[key][0]:
                    kv_store[key] = (ts, new_grade)
            
            print(f"Fetched {len(kv_store)} records from MongoDB.")

        else:
            print(f"Merge from {source_system} not supported yet.")
            return
        
        # Step 2: Update MySQL (grades table) using set() method
        for (student_id, course_id), (ts, new_grade) in kv_store.items():
            self.set(student_id, course_id, float(new_grade))
        
        print(f"Merged {len(kv_store)} records into MySQL from {source_system.upper()}.")
