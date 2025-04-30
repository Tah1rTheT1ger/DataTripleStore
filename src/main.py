from hive_manager import HiveGradeManager
from postgres_manager import SQLGradeManager
from mongo_manager import MongoDBGradeManager
import time

# Initialize managers
hive_mgr = HiveGradeManager('student_course_grades.csv')
sql_mgr = SQLGradeManager('postgresql+psycopg2://postgres:SQL%40123@localhost:5432/new_database', 'student_course_grades.csv')
mongo_mgr = MongoDBGradeManager('student_course_grades.csv')

# Manager lookup
manager_map = {
    "HIVE": hive_mgr,
    "SQL": sql_mgr,
    "MONGO": mongo_mgr
}

# Open and read the input file
with open('testcase_hive.in', 'r') as f:
    lines = f.readlines()

for line in lines:
    line = line.strip()
    if not line:
        continue

    if '.' in line:
        system, rest = line.split('.', 1)
        system = system.strip().upper()
        rest = rest.strip()
        time.sleep(1)


        if rest.startswith("SET"):
            # Format: SET (( SID103 , CSE016 ) , A )
            inside = rest[len("SET (("):-2]  # remove 'SET ((' and last ')'
            id_part_end = inside.find(')')
            ids_part = inside[:id_part_end]
            grade = inside[id_part_end+2:].lstrip(',').strip()

            student_id, course_id = [x.strip() for x in ids_part.split(',')]

            grade = grade.strip()
            manager_map[system].set(student_id, course_id, grade)
            print(f"{system}: SET ({student_id}, {course_id}) -> {grade}")

        elif rest.startswith("GET"):
            # Format: GET ( SID103 , CSE016 )
            inside = rest[len("GET ("):-1]  # remove 'GET (' and final ')'
            student_id, course_id = [x.strip() for x in inside.split(',')]
            grade = manager_map[system].get(student_id, course_id)
            print(f"{system}: GET ({student_id}, {course_id}) -> {grade}")

        elif rest.startswith("MERGE"):
            # Format: MERGE ( SQL )
            target = rest[len("MERGE ("):-1].strip()
            manager_map[system].merge(target)
            print(f"{system}: MERGE ({target})")

        else:
            print(f"Unknown operation: {rest}")

    else:
        print(f"Invalid format: {line}")
