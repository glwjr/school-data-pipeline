# School Data Pipeline

An ETL pipeline for processing student enrollment data, built with Python, SQLite, and pandas.

## Project Overview

This pipeline processes student enrollment data from CSV files, stores it in a relational database, and generates automated analytics reports.

### Key Features

- **ETL Pipeline**: Automated data extraction, transformation, and loading
- **Data Validation**: Comprehensive data quality checks and error handling
- **Analytics Engine**: Automated generation of enrollment insights
- **Report Generation**: Timestamped CSV reports
- **Professional Logging**: Centralized logging with file and console output
- **Modular Architecture**: Clean, maintainable code structure

## Architecture

```
school-data-pipeline/
├── src/
│   ├── config/           # Configuration management
│   │   ├── directories.py    # Path configurations
│   │   ├── pipeline.py       # Pipeline settings
│   │   └── logging.py        # Logging setup
│   ├── analytics.py      # Data analysis functions
│   ├── database.py       # Database operations
│   ├── generate_data.py  # Mock data generation
│   └── pipeline.py       # Main ETL orchestration
├── data/
│   ├── raw/             # Source CSV files
│   └── processed/       # SQLite database
├── logs/                # Pipeline execution logs
└── reports/             # Generated analytics reports
```

## Quick Start

### Prerequisites

- Python 3.8+
- pip

### Installation

1. Clone the repository:

```bash
git clone https://github.com/glwjr/school-data-pipeline.git
cd school-data-pipeline
```

2. Create and activate virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

### Running the Pipeline

1. **Generate sample data** (optional - for demo purposes):

```bash
python src/generate_data.py
```

2. **Run the full ETL pipeline**:

```bash
python src/pipeline.py
```

The pipeline will:

- Create the SQLite database and tables
- Load student and enrollment data
- Run data validation checks
- Generate analytics and summary reports
- Export results to timestamped CSV files

## Sample Output

### Console Output

```
2025-06-03 11:25:17,837 - config.logging - INFO - Starting ETL pipeline...
2025-06-03 11:25:17,837 - config.logging - INFO - Data files not found. Generating sample data...
2025-06-03 11:25:17,837 - config.logging - INFO - Generating mock student data...
2025-06-03 11:25:17,888 - config.logging - INFO - Generated:
2025-06-03 11:25:17,888 - config.logging - INFO - - 100 students
2025-06-03 11:25:17,888 - config.logging - INFO - - 450 enrollment records
2025-06-03 11:25:17,888 - config.logging - INFO - Files saved locally to data/raw
2025-06-03 11:25:17,888 - config.logging - INFO - Sample data generated successfully!
2025-06-03 11:25:17,888 - config.logging - INFO - Connecting to database...
2025-06-03 11:25:17,890 - config.logging - INFO - Connected to database!
2025-06-03 11:25:17,890 - config.logging - INFO - Creating tables in database...
2025-06-03 11:25:17,892 - config.logging - INFO - Tables successfully created!
2025-06-03 11:25:17,892 - config.logging - INFO - Clearing existing data...
2025-06-03 11:25:17,893 - config.logging - INFO - Tables cleared!
2025-06-03 11:25:17,893 - config.logging - INFO - Database connection closed.
2025-06-03 11:25:17,893 - config.logging - INFO - Loading students data...
2025-06-03 11:25:17,895 - config.logging - INFO - Connecting to database...
2025-06-03 11:25:17,895 - config.logging - INFO - Connected to database!
2025-06-03 11:25:17,895 - config.logging - INFO - Inserting 100 student records...
2025-06-03 11:25:17,896 - config.logging - INFO - Student data inserted successfully!
```

### Generated Reports

- `course_popularity.csv` - Course enrollment rankings
- `enrollment_by_school.csv` - Student counts per school
- `students_by_grade.csv` - Grade level distribution

## Configuration

The pipeline is highly configurable through the `src/config/` module:

### Data Settings (`config/pipeline.py`)

- Number of students to generate
- Available courses and grade levels
- School configurations
- Academic periods

### Path Settings (`config/directories.py`)

- Data input/output locations
- Database file location
- Report export directories

## 📈 Analytics Capabilities

### Enrollment Analysis

- Students per school breakdown
- Grade level distribution
- Cross-school comparisons

### Course Analytics

- Course popularity rankings
- Enrollment patterns by semester
- Course offering analysis

### Summary Reporting

- Total student and enrollment counts
- Average courses per student
- Largest school identification
- Most popular course detection

## Technical Details

### Database Schema

```sql
students:
- student_id (INTEGER PRIMARY KEY)
- grade_level (INTEGER NOT NULL)
- school_id (INTEGER NOT NULL)
- enrollment_date (TEXT NOT NULL)

enrollments:
- enrollment_id (INTEGER PRIMARY KEY)
- student_id (INTEGER NOT NULL)
- school_id (INTEGER NOT NULL)
- course_name (TEXT NOT NULL)
- semester (TEXT NOT NULL)
- FOREIGN KEY (student_id) REFERENCES students
```

### Data Quality Checks

- Required column validation
- Foreign key integrity verification

## License

MIT License - feel free to use this code for your own projects.
