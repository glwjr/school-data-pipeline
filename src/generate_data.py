import os

import numpy as np
import pandas as pd

from config import (
    AVAILABLE_COURSES,
    GRADE_LEVELS,
    MAX_COURSES_PER_STUDENT,
    MIN_COURSES_PER_STUDENT,
    NUM_STUDENTS,
    RAW_DATA_DIR,
    SCHOOL_IDS,
    SEMESTERS,
)


def generate_mock_data():
    """Generate mock education data for pipeline"""

    print("Generating mock student data...")

    # Students data
    students = pd.DataFrame(
        {
            "student_id": range(1, NUM_STUDENTS + 1),
            "grade_level": np.random.choice(GRADE_LEVELS, NUM_STUDENTS),
            "school_id": np.random.choice(SCHOOL_IDS, NUM_STUDENTS),
            "enrollment_date": pd.date_range(
                "2024-08-15", periods=NUM_STUDENTS, freq="D"
            )[:NUM_STUDENTS],
        }
    )

    # Enrollment data
    enrollments = []
    for student_id in range(1, NUM_STUDENTS + 1):
        num_courses = np.random.randint(
            MIN_COURSES_PER_STUDENT, MAX_COURSES_PER_STUDENT
        )
        student_courses = np.random.choice(
            AVAILABLE_COURSES,
            size=num_courses,
            replace=False,
        )

        for course in student_courses:
            enrollments.append(
                {
                    "enrollment_id": len(enrollments) + 1,
                    "student_id": student_id,
                    "school_id": students[students["student_id"] == student_id][
                        "school_id"
                    ].iloc[0],
                    "course_name": course,
                    "semester": np.random.choice(SEMESTERS),
                }
            )

    enrollments_df = pd.DataFrame(enrollments)

    # Save locally
    os.makedirs(RAW_DATA_DIR, exist_ok=True)
    students.to_csv(f"{RAW_DATA_DIR}/students.csv", index=False)
    enrollments_df.to_csv(f"{RAW_DATA_DIR}/enrollments.csv", index=False)

    print("Generated:")
    print(f"- {len(students)} students")
    print(f"- {len(enrollments_df)} enrollment records")
    print(f"Files saved locally to {RAW_DATA_DIR}")

    return students, enrollments_df


if __name__ == "__main__":
    generate_mock_data()
