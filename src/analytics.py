import pandas as pd

from database import close_connection, create_connection
from logging_config import logger


def enrollment_by_school():
    """Analyze student enrollment by school"""
    logger.info("Analyzing enrollment by school...")
    conn = create_connection()
    if conn:
        try:
            # Count unique students per school
            sql = """
                SELECT school_id,
                    COUNT(DISTINCT student_id) as student_count
                FROM enrollments
                GROUP BY school_id
                ORDER BY student_count DESC
            """

            result = pd.read_sql(sql, conn)
            logger.info(
                f"Enrollment analysis completed - {len(result)} schools analyzed"
            )

            # Log the results
            for _, row in result.iterrows():
                logger.info(
                    f"School {row['school_id']}: {row['student_count']} students"
                )

            return result

        except Exception as e:
            logger.error(f"Enrollment analysis failed: {e}")
            return None
        finally:
            close_connection(conn)


def students_by_grade():
    """Analyze student distribution by grade level"""
    logger.info("Analyzing student distribution by grade level...")
    conn = create_connection()
    if conn:
        try:
            # Count unique students per grade
            sql = """
                SELECT grade_level,
                    COUNT(DISTINCT student_id) as student_count
                FROM students
                GROUP BY grade_level
                ORDER BY grade_level ASC
            """

            result = pd.read_sql(sql, conn)
            logger.info(
                f"Student distribution analysis completed - {len(result)} grades analyzed"
            )

            # Log the results
            for _, row in result.iterrows():
                logger.info(
                    f"Grade {row['grade_level']}: {row['student_count']} students"
                )

            return result

        except Exception as e:
            logger.error(f"Student distribution analysis failed: {e}")
            return None
        finally:
            close_connection(conn)


def course_popularity():
    """Analyze most popular courses"""
    logger.info("Analyzing most popular courses...")
    conn = create_connection()
    if conn:
        try:
            # Measure and order the most popular courses
            sql = """
                SELECT course_name,
                    COUNT(DISTINCT student_id) as enrollment_count
                FROM enrollments
                GROUP BY course_name
                ORDER BY enrollment_count DESC
            """

            result = pd.read_sql(sql, conn)
            logger.info(
                f"Course popularity analysis completed - {len(result)} courses analyzed"
            )

            # Log the results
            for _, row in result.iterrows():
                logger.info(
                    f"Course {row['course_name']}: {row['enrollment_count']} students enrolled"
                )

            return result

        except Exception as e:
            logger.error(f"Course popularity analysis failed: {e}")
            return None
        finally:
            close_connection(conn)
