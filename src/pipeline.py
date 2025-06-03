import pandas as pd

from database import (
    clear_tables,
    close_connection,
    create_connection,
    create_tables,
    insert_enrollments,
    insert_students,
)

STUDENTS_DATA_PATH = "data/raw/students.csv"
ENROLLMENTS_DATA_PATH = "data/raw/enrollments.csv"


def load_students_data():
    """Load students data from CSV to database"""
    print("Loading students data...")
    df = pd.read_csv(STUDENTS_DATA_PATH)

    required_columns = ["student_id", "grade_level", "school_id", "enrollment_date"]
    if not all(col in df.columns for col in required_columns):
        print("Missing required columns in students data!")
        return

    conn = create_connection()
    if conn:
        insert_students(conn, df)
        close_connection(conn)


def load_enrollments_data():
    """Load enrollments data from CSV to database"""
    print("Loading enrollments data...")
    df = pd.read_csv(ENROLLMENTS_DATA_PATH)

    required_columns = [
        "enrollment_id",
        "student_id",
        "school_id",
        "course_name",
        "semester",
    ]
    if not all(col in df.columns for col in required_columns):
        print("Missing required columns in enrollments data!")
        return

    conn = create_connection()
    if conn:
        insert_enrollments(conn, df)
        close_connection(conn)


def validate_pipeline():
    """Validate the ETL pipeline results"""
    print("\n--- Pipeline Validation ---")
    conn = create_connection()
    if conn:
        try:
            # Check record counts
            students_count = pd.read_sql(
                "SELECT COUNT(*) as count FROM students", conn
            ).iloc[0, 0]
            enrollments_count = pd.read_sql(
                "SELECT COUNT(*) as count FROM enrollments", conn
            ).iloc[0, 0]

            print(f"✓ Students loaded: {students_count}")
            print(f"✓ Enrollments loaded: {enrollments_count}")

            # Check data relationships
            orphaned = pd.read_sql(
                """
                SELECT COUNT(*) as count 
                FROM enrollments e 
                LEFT JOIN students s ON e.student_id = s.student_id 
                WHERE s.student_id IS NULL
            """,
                conn,
            ).iloc[0, 0]

            print(f"✓ Orphaned enrollments: {orphaned}")

        except Exception as e:
            print(f"✗ Validation failed: {e}")
        finally:
            close_connection(conn)


def main():
    """Run the full ETL pipeline"""
    print("Starting ETL pipeline...")

    # Set up database
    conn = create_connection()
    if conn:
        create_tables(conn)
        clear_tables(conn)
        close_connection(conn)

    # Load data
    load_students_data()
    load_enrollments_data()

    # Validate results
    validate_pipeline()

    print("ETL pipeline completed!")


if __name__ == "__main__":
    main()
