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

- Clean the `study_program_name` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces, and
  converting the text to sentence case

#### Extraction Stage

- Extract `study_program_code` from `study_program_url` and `study_program_duration`. The `study_program_code` is the last part of the
  `study_program_url` concatenated with the `study_program_duration`

##### Generation Stage

- Generate the `study_program_id` column by indexing column `study_program_code`

#### Curriculum:

##### Cleaning Stage

- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces
- Clean the `study_program_name` and `course_name_mk` columns by removing any leading or trailing whitespaces, as well as occurrences of
  multiple whitespaces, and converting the text to sentence case

#### Courses:

##### Cleaning Stage

- Read the courses data from the `courses.csv` file
- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces
- Clean the `course_name_en` and `course_name_mk` columns by removing any leading or trailing whitespaces, as well as occurrences of
  multiple whitespaces, and converting the text to sentence case
- Clean the `course_professors` and `course_prerequisite` columns by replacing newline characters with commas, removing any leading or
  trailing whitespaces, as well as occurrences of multiple whitespaces and replacing `nulls` with `нема`
- Clean the `course_professors` column by splitting the values and removing the academic titles

##### Extraction Stage

- Extract the `course_level` column from the `course_code` column
- Extract the `course_semester` column from the columns `course_season` and `course_academic_year`
- Extract the `course_prerequisite_type` column from the `course_prerequisite` column
- Extract the `course_prerequisites_minimum_required_courses` column from the columns `course_prerequisites` and `course_prerequisite_type`

##### Transformation Stage

- Transform the `course_prerequisite` column by splitting the values and validating the course names and calculating the minimum number
  of subjects that need to be passed in order to enroll in the course

#### Flattening Stage

- Flatten the `course_professors` column by splitting the values and creating a new row for each professor
- Flatten the `course_prerequisite` column by splitting the values and creating a new row for each prerequisite

##### Generation Stage

- Generate the `course_id` column by indexing the courses
- Generate the `course_professor_id` column by indexing the professors
- Generate the `course_prerequisite_id` column by indexing the prerequisites
- Generate the `course_prerequisites_course_id` by joining the `course_id` and `course_prerequisites` columns

### Merged Data:

- Merge `study_programs` and `curriculum` on `study_program_name`, `study_program_duration` and `study_program_url` columns
- Merge the merged data from the previous step with `courses` on `course_code` and `course_name_mk` columns

### Results:

This ETL application will save the transformed data in four different files:

- `study_programs.csv`: contains the details of the study programs
- `curriculum.csv`: contains the details of the study programs and related courses
- `courses.csv`: contains the details of the courses
- `merged_data.csv`: contains the merged data from `curriculum.csv` and `courses.csv`

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the scraper, make sure to set the following environment variables:

- `FILE_STORAGE_TYPE`: the type of storage to use (either `LOCAL` or `MINIO`)
- `STUDY_PROGRAMS_INPUT_DATA_FILE_PATH`: the path to the study programs data file
- `CURRICULA_INPUT_DATA_FILE_PATH`: the path to the curricula data file
- `COURSE_INPUT_DATA_FILE_PATH`: the path to the courses data file
- `STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME`: the name of the study programs output file
- `CURRICULA_DATA_OUTPUT_FILE_NAME`: the name of the curricula output file
- `COURSE_DATA_OUTPUT_FILE_NAME`: the name of the courses output file
- `MERGED_DATA_OUTPUT_FILE_NAME`: the name of the merged output file


##### If running the application with local storage:

- `INPUT_DIRECTORY_PATH`: the path to the directory where the input files are stored
- `OUTPUT_DIRECTORY_PATH`: the path to the directory where the output files will be saved

##### If running the application with MinIO:

- `MINIO_ENDPOINT_URL`: the endpoint of the MinIO server
- `MINIO_ACCESS_KEY`: the access key of the MinIO server
- `MINIO_SECRET_KEY`: the secret key of the MinIO server
- `MINIO_SOURCE_BUCKET_NAME`: the name of the bucket where the input files are stored
- `MINIO_DESTINATION_BUCKET_NAME`: the name of the bucket where the output files will be saved

## Installation

1. Clone the repository
    ```bash
    git clone <repository_url>
    ```

2. Install the required packages
    ```bash
    pip install -r requirements.txt
    ```

3. Run the application
    ```bash
    python main.py
    ```

Make sure to replace `<repository_url>` with the actual URL of the repository.
