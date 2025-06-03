"""Directory and path configuration for the school data pipeline"""

import os

# Base directories
DATA_DIR = "data"
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, "processed")
LOGS_DIR = "logs"
REPORTS_DIR = "reports"

# Data file paths
STUDENTS_CSV_PATH = os.path.join(RAW_DATA_DIR, "students.csv")
ENROLLMENTS_CSV_PATH = os.path.join(RAW_DATA_DIR, "enrollments.csv")

# Database path
DATABASE_PATH = os.path.join(PROCESSED_DATA_DIR, "school_data.db")
