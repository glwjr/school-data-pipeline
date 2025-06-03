from datetime import datetime
import os

import pandas as pd

from config import logger, REPORTS_DIR
from database import close_connection, create_connection


def enrollment_by_school():
    """Analyze student enrollment by school"""
    logger.info("Analyzing enrollment by school...")
    conn = create_connection()
    if conn:
        try:
            # Count unique students per school
            sql = """
                SELECT school_id, COUNT(DISTINCT student_id) as student_count
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
                SELECT grade_level, COUNT(DISTINCT student_id) as student_count
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
                SELECT course_name, COUNT(DISTINCT student_id) as enrollment_count
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


def generate_summary_report():
    """Generate a comprehensive summary report of the school data"""
    logger.info("Generating summary report...")
    conn = create_connection()
    if conn:
        try:
            # Collect key metrics
            total_students = pd.read_sql(
                "SELECT COUNT(*) as count FROM students", conn
            )["count"].values[0]
            total_schools = pd.read_sql(
                "SELECT COUNT(DISTINCT school_id) as count FROM students", conn
            )["count"].values[0]
            total_courses = pd.read_sql(
                "SELECT COUNT(DISTINCT course_name) as count FROM enrollments", conn
            )["count"].values[0]
            total_enrollments = pd.read_sql(
                "SELECT COUNT(*) as count FROM enrollments", conn
            )["count"].values[0]

            # Calculate averages
            avg_courses_per_student = (
                total_enrollments / total_students if total_students > 0 else 0
            )

            # Get top insights
            largest_school = pd.read_sql(
                """
                    SELECT school_id, COUNT(*) as student_count
                    FROM students
                    GROUP BY school_id
                    ORDER BY student_count DESC
                    LIMIT 1
                """,
                conn,
            )

            most_popular_course = pd.read_sql(
                """
                    SELECT course_name, COUNT(DISTINCT student_id) as enrollment_count
                    FROM enrollments
                    GROUP BY course_name
                    ORDER BY enrollment_count DESC
                    LIMIT 1
                """,
                conn,
            )

            # Generate report
            logger.info("\n--- School Data Summary Report ---")
            logger.info(f"Total Students: {total_students}")
            logger.info(f"Total Schools: {total_schools}")
            logger.info(f"Total Courses: {total_courses}")
            logger.info(f"Total Enrollments: {total_enrollments}")
            logger.info(f"Average Courses per Student: {avg_courses_per_student:.1f}")

            if not largest_school.empty:
                logger.info(
                    f"Largest School: School {largest_school.iloc[0]['school_id']} ({largest_school.iloc[0]['student_count']} students)"
                )

            if not most_popular_course.empty:
                logger.info(
                    f"Most Popular Course: {most_popular_course.iloc[0]['course_name']} ({most_popular_course.iloc[0]['enrollment_count']} students)"
                )

            logger.info("\n--- End Report ---")

            return {
                "total_students": total_students,
                "total_schools": total_schools,
                "total_courses": total_courses,
                "avg_courses_per_student": avg_courses_per_student,
                "largest_school": largest_school,
                "most_popular_course": most_popular_course,
            }

        except Exception as e:
            logger.error(f"Summary report generation failed: {e}")
            return None
        finally:
            close_connection(conn)


def export_analytics_reports():
    """Export all analytics to CSV files with timestamps"""

    # Create reports directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    reports_dir = os.path.join(REPORTS_DIR, timestamp)
    os.makedirs(reports_dir, exist_ok=True)

    try:
        # Run analytics and export
        enrollment_data = enrollment_by_school()
        grade_data = students_by_grade()
        course_data = course_popularity()

        # Export to CSV
        if enrollment_data is not None:
            enrollment_data.to_csv(
                f"{reports_dir}/enrollment_by_school.csv", index=False
            )
            logger.info(f"Exported enrollment analysis")

        if grade_data is not None:
            grade_data.to_csv(f"{reports_dir}/students_by_grade.csv", index=False)
            logger.info(f"Exported grade analysis")

        if course_data is not None:
            course_data.to_csv(f"{reports_dir}/course_popularity.csv", index=False)
            logger.info(f"Export course analysis")

        logger.info(f"All reports exported to {reports_dir}/")
        return reports_dir

    except Exception as e:
        logger.error(f"Failed to export reports: {e}")
        return None
