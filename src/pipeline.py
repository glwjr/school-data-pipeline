import pandas as pd

from analytics import (
    course_popularity,
    enrollment_by_school,
    export_analytics_reports,
    generate_summary_report,
    students_by_grade,
)
from database import (
    clear_tables,
    close_connection,
    create_connection,
    create_tables,
    insert_enrollments,
    insert_students,
)
from config import logger, ENROLLMENTS_CSV_PATH, STUDENTS_CSV_PATH


def load_students_data():
    """Load students data from CSV to database"""
    logger.info("Loading students data...")
    df = pd.read_csv(STUDENTS_CSV_PATH)

    required_columns = ["student_id", "grade_level", "school_id", "enrollment_date"]
    if not all(col in df.columns for col in required_columns):
        logger.error("Missing required columns in students data!")
        return

    conn = create_connection()
    if conn:
        insert_students(conn, df)
        close_connection(conn)


def load_enrollments_data():
    """Load enrollments data from CSV to database"""
    logger.info("Loading enrollments data...")
    df = pd.read_csv(ENROLLMENTS_CSV_PATH)

    required_columns = [
        "enrollment_id",
        "student_id",
        "school_id",
        "course_name",
        "semester",
    ]
    if not all(col in df.columns for col in required_columns):
        logger.error("Missing required columns in enrollments data!")
        return

    conn = create_connection()
    if conn:
        insert_enrollments(conn, df)
        close_connection(conn)


def validate_pipeline():
    """Validate the ETL pipeline results"""
    logger.info("\n--- Pipeline Validation ---")
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

            logger.info(f"✓ Students loaded: {students_count}")
            logger.info(f"✓ Enrollments loaded: {enrollments_count}")

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

            logger.info(f"✓ Orphaned enrollments: {orphaned}")

        except Exception as e:
            logger.error(f"✗ Validation failed: {e}")
        finally:
            close_connection(conn)


def run_analytics():
    """Run all analytics and generate insights"""
    logger.info("Running data analytics...")

    # Run all analytics
    enrollment_data = enrollment_by_school()
    grade_data = students_by_grade()
    course_data = course_popularity()
    summary = generate_summary_report()

    # Export reports
    reports_dir = export_analytics_reports()
    if reports_dir:
        logger.info(f"Report available in: {reports_dir}")

    logger.info("Analytics completed!")
    return enrollment_data, grade_data, course_data, summary


def main():
    """Run the full ETL pipeline"""
    logger.info("Starting ETL pipeline...")

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

    # Run analytics
    run_analytics()

    logger.info("ETL pipeline completed!")


if __name__ == "__main__":
    main()
