import os

import numpy as np
import pandas as pd


def generate_mock_data():
    """Generate mock education data for pipeline"""

    print("Generating mock student data...")

    # Students data
    students = pd.DataFrame(
        {
            "student_id": range(1, 101),
            "grade_level": np.random.choice([6, 7, 8], 100),
            "school_id": np.random.choice([1, 2, 3], 100),
            "enrollment_date": pd.date_range("2024-08-15", periods=100, freq="D")[:100],
        }
    )

    # Enrollment data
    enrollments = []
    for student_id in range(1, 101):
        num_courses = np.random.randint(3, 7)  # 3 - 6 enrollments per student
        student_courses = np.random.choice(
            [
                "Algebra I",
                "Geometry",
                "Biology",
                "World History",
                "AP English",
                "PE",
                "Art",
                "Computer Science",
            ],
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
                    "semester": np.random.choice(["Fall 2024", "Spring 2025"]),
                }
            )

    enrollments_df = pd.DataFrame(enrollments)

    # Save locally
    os.makedirs("data/raw", exist_ok=True)
    students.to_csv("data/raw/students.csv", index=False)
    enrollments_df.to_csv("data/raw/enrollments.csv", index=False)

    print("Generated:")
    print(f"- {len(students)} students")
    print(f"- {len(enrollments_df)} enrollment records")
    print("Files saved locally to data/raw")

    return students, enrollments_df


if __name__ == "__main__":
    generate_mock_data()
