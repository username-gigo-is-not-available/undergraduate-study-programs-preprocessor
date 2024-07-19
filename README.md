# FCSE-Skopje 2023 Undergraduate Study Programs ETL

The ETL application is used to transform the study programs and related courses from
the [Faculty of Computer Science and Engineering](https://finki.ukim.mk) at
the [Ss. Cyril and Methodius University in Skopje](https://www.ukim.edu.mk).
which can be found at the following [URL](https://finki.ukim.mk/mk/dodiplomski-studii).

## Prerequisites

- Data from
  the [undergraduate-study-program-scraper](https://github.com/username-gigo-is-not-available/undergraduate-study-programs-scraper) is
  required to run this ETL application.

## Overview

### Pipeline:

#### Study Programs:

##### Cleaning Stage

- Read the study programs data from the `study_programs.csv` file
- Clean the `study_program_name` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces, and
  converting the text to sentence case

#### Curriculum:

##### Cleaning Stage

- Read the curriculum data from the `curriculum.csv` file
- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces
- Clean the `study_program_name` and `course_name_mk` columns by removing any leading or trailing whitespaces, as well as occurrences of
  multiple whitespaces, and converting the text to sentence case

##### Handling Invalid Data

- Handle invalid `course_code` values by extracting the course code from the `course_name_mk` column, as well as removing the
  valid `course_code` from `course_name_mk`

#### Courses:

##### Cleaning Stage

- Read the courses data from the `courses.csv` file
- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces
- Clean the `course_name_en` and `course_name_mk` columns by removing any leading or trailing whitespaces, as well as occurrences of
  multiple whitespaces, and converting the text to sentence case
- Clean the `course_professors` and `course_prerequisite` columns by replacing newline characters with commas, removing any leading or
  trailing whitespaces, as well as occurrences of multiple whitespaces and replacing `nulls` with `нема`

##### Handling Invalid Data

- Handle invalid `course_code` values by extracting the course code from the `course_name_mk` column, as well as removing the
  valid `course_code` from `course_name_mk` and `course_name_en`

##### Extraction Stage

- Extract the `course_level` column from the `course_code` column
- Extract the `course_semester` column from the columns `course_season` and `course_academic_year`
- Extract the `course_prerequisite_type` column from the `course_prerequisite` column

#### Transformation Stage

- Transform the `course_professors` column by splitting the values and removing the academic titles
- Transform the `course_prerequisite` column by splitting the values and validating the course names and calculating the minimum number
  of subjects that need to be passed in order to enroll in the course

### Results:

This ETL application will save the transformed data in four different files:

- `study_programs.csv`: contains the details of the study programs
- `curriculum.csv`: contains the details of the study programs and related courses
- `courses.csv`: contains the details of the courses
- `merged.csv`: contains the merged data from `curriculum.csv` and `courses.csv`

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the scraper, make sure to set the following environment variables:

- `OUTPUT_DIRECTORY_PATH`: the path to the directory where the output files will be saved
- `STUDY_PROGRAMS_INPUT_DATA_FILE_PATH`: the path to the study programs data file
- `CURRICULA_INPUT_DATA_FILE_PATH`: the path to the curricula data file
- `COURSE_INPUT_DATA_FILE_PATH`: the path to the courses data file
- `STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME`: the name of the study programs output file
- `CURRICULA_DATA_OUTPUT_FILE_NAME`: the name of the curricula output file
- `COURSE_DATA_OUTPUT_FILE_NAME`: the name of the courses output file
- `MERGED_DATA_OUTPUT_FILE_NAME`: the name of the merged output file
- `MAX_WORKERS`: the number of threads that will be used to read and write the data


## Installation

1. Clone the repository
    ```bash
    git clone <repository_url>
    ```

2. Install the required packages
    ```bash
    pip install -r requirements.txt
    ```

3. Run the scraper
    ```bash
    python main.py
    ```

Make sure to replace `<repository_url>` with the actual URL of the repository.
