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


def clear_tables(conn):
    """Clear all data from tables"""
    try:
        print("Clearing existing data...")
        conn.execute("DELETE FROM enrollments")
        conn.execute("DELETE FROM students")
        conn.commit()
        print("Tables cleared!")
    except Exception as e:
        print(f"Failed to clear tables: {e}")


def close_connection(conn):
    """Close database connection"""
    if conn:
        conn.close()
        print("Database connection closed.")


def insert_students(conn, students_df):
    """Insert student data into database"""
    try:
        print(f"Inserting {len(students_df)} student records...")
        students_df.to_sql("students", conn, if_exists="append", index=False)
        print("Student data inserted successfully!")
    except Exception as e:
        print(f"Failed to insert student data: {e}")


def insert_enrollments(conn, enrollments_df):
    """Insert enrollment data into database"""
    try:
        print(f"Inserting {len(enrollments_df)} enrollment records...")
        enrollments_df.to_sql("enrollments", conn, if_exists="append", index=False)
        print("Enrollment data inserted successfully!")
    except Exception as e:
        print(f"Failed to insert enrollment data: {e}")
