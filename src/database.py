import os
import sqlite3

DB_PATH = "data/processed/school_data.db"


def create_connection():
    """Connect to SQLite database"""
    try:
        print("Connecting to database...")

        # Ensure directory exists
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

        conn = sqlite3.connect(DB_PATH)
        print("Connected to database!")
        return conn
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        return None


def create_tables(conn):
    """Create tables in SQLite database"""
    try:
        print("Creating tables in database...")
        cur = conn.cursor()
        sql = """
            BEGIN;
            CREATE TABLE IF NOT EXISTS students(
                student_id INTEGER PRIMARY KEY, 
                grade_level INTEGER NOT NULL, 
                school_id INTEGER NOT NULL, 
                enrollment_date TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS enrollments(
                enrollment_id INTEGER PRIMARY KEY,
                student_id INTEGER NOT NULL,
                school_id INTEGER NOT NULL,
                course_name TEXT NOT NULL,
                semester TEXT NOT NULL,
                FOREIGN KEY (student_id) REFERENCES students (student_id)
            );
            COMMIT;
        """
        cur.executescript(sql)
        print("Tables successfully created!")
    except Exception as e:
        print(f"Failed to create tables in database: {e}")


def close_connection(conn):
    """Close database connection"""
    if conn:
        conn.close()
        print("Database connection closed.")


conn = create_connection()
create_tables(conn)
close_connection(conn)
