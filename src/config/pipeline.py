"""Pipeline configuration settings"""

# Data generation settings
NUM_STUDENTS = 100
GRADE_LEVELS = [6, 7, 8]
SCHOOL_IDS = [1, 2, 3]
MIN_COURSES_PER_STUDENT = 3
MAX_COURSES_PER_STUDENT = 7

# Course offerings
AVAILABLE_COURSES = [
    "Algebra I",
    "Geometry",
    "Biology",
    "World History",
    "AP English",
    "PE",
    "Art",
    "Computer Science",
]

# Academic periods
SEMESTERS = ["Fall 2024", "Spring 2025"]

# Database settings
DB_TIMEOUT = 30
MAX_RETRIES = 3

# Logging settings
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Report settings
EXPORT_REPORTS = True
INCLUDE_SUMMARY = True
