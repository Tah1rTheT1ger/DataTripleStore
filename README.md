# Data Triple Store Project

This project demonstrates data integration and synchronization across heterogeneous database systems, specifically focusing on Hive, PostgreSQL, and MongoDB. It manages student course grades with the same dataset stored across these systems and ensures consistency via CRUD operations and merge functionality. The system leverages operation logs (oplogs) to maintain eventual consistency across all platforms.

## Project Overview

The primary goal of this project is to implement consistent **CRUD** (Create, Read, Update, Delete) operations across three different database systems and ensure eventual consistency through merge functionality. The dataset consists of student course grades, with the following schema:

- **Student ID**
- **Course ID**
- **Student Name**
- **Mail**
- **Grade**

The operation logs track **GET** and **SET** operations, and the **merge** function ensures that the state of each system is synchronized with others.

## Database Systems Used

1. **Hive**
   - **Create:** `CREATE TABLE`
   - **Read:** `SELECT Grade FROM table WHERE student_ID='...' AND course_ID='...'`
   - **Update:** `INSERT OVERWRITE TABLE`
   - **Delete:** `DROP TABLE`

2. **PostgreSQL**
   - **Create:** `CREATE`
   - **Read:** `SELECT Grade FROM table WHERE student_ID='...' AND course_ID='...`
   - **Update:** `UPDATE table SET Grade='...' WHERE student_ID='...' AND course_ID='...`
   - **Delete:** `DELETE`

3. **MongoDB**
   - **Create:** `createCollection`, `insertOne`, `insertMany`
   - **Read:** `findOne({ student_ID, course_ID }, { Grade })`
   - **Update:** `updateOne({ student_ID, course_ID }, { $set: { Grade } })`
   - **Delete:** `deleteOne`


## Features

- **CRUD Operations:** Implemented for `Grade` field in all three systems.
- **Operation Log (Oplog):** Records **GET** and **SET** operations with timestamps.
- **Merge Functionality:** Synchronizes updates from one system to another using the oplog.
- **Eventual Consistency:** The merge operations ensure that all systems converge to the same state, even if the operations are performed in different orders.

## System Architecture

The system consists of four Python modules:

1. **`main.py`** - Central orchestration script that handles CRUD and merge operations.
2. **`hive_manager.py`** - Manages Hive database operations.
3. **`postgres_manager.py`** - Manages PostgreSQL database operations.
4. **`mongo_manager.py`** - Manages MongoDB database operations.

Each module provides functions for **GET**, **SET**, and **MERGE** operations, and each manager maintains its own operation log.

## Merge Functionality

The **merge** function synchronizes updates between two systems. It compares the timestamps of operations in the oplogs and applies the most recent change to the calling system. The merge operation ensures the following properties:

- **Commutativity**: The order of merging operations does not affect the final state.
- **Associativity**: The grouping of merge operations does not affect the final state.
- **Idempotency**: Merging a system with itself leaves it unchanged.
- **Convergence**: Once all systems are merged, they reach consistency.

## Code Explanation

### `main.py`
- Initializes connections to Hive, PostgreSQL, and MongoDB.
- Reads operations from `testcase_hive.in`.
- Dispatches operations to the corresponding database manager.

### `hive_manager.py`
- Manages Hive operations, including table initialization, data retrieval, update, and merge functionality.

### `postgres_manager.py`
- Manages PostgreSQL operations, including table initialization, data retrieval, update, and merge functionality.

### `mongo_manager.py`
- Manages MongoDB operations, including document retrieval, update, and merge functionality.

## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/Tah1rTheT1ger/DataTripleStore.git
   cd database-synchronization

## Contributors

- IMT2022034 - Tarun Kondapalli Srivatsa  
- IMT2022065 - Amruth Gadepalli  
- IMT2022100 - Tahir Mohammed Khadarabad
