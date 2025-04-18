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

#### Study Program:

##### Loading Stage

- Load the study programs data (output from the scraper) with the following columns:
  `study_program_name`, `study_program_url`, `study_program_duration`

##### Cleaning Stage

- Clean the `study_program_name` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces, and
  converting the text to sentence case

#### Extraction Stage

- Extract `study_program_code` from `study_program_url` and `study_program_duration`. The `study_program_code` is the last part of the
  `study_program_url` concatenated with the `study_program_duration`

##### Generation Stage

- Generate the `study_program_id` column by indexing column `study_program_code`

#### Storing Stage

- Store the cleaned data in a CSV file with the following
  columns: `study_program_id`, `study_program_code`, `study_program_name`, `study_program_duration`,
  `study_program_url`

#### Course-Professor:

##### Loading Stage

- Load the courses data (output from the scraper) with the following columns:
  `course_code`, `course_name_mk`, `course_name_en`, `course_url`, `course_professors`

##### Cleaning Stage

- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces
- Clean the `course_name_en` and `course_name_mk` columns by removing any leading or trailing whitespaces, as well as occurrences of
  multiple whitespaces, and converting the text to sentence case
- Clean the `course_professors` column by replacing newline characters with commas, removing any leading or
  trailing whitespaces, as well as occurrences of multiple whitespaces and replacing `nulls` with `нема`.
  Remove titles and degrees from the names of the professors, then concatenate the processed values with the pipe(`|`) separator

#### Flattening Stage

- Flatten the `course_professors` column by splitting the values and creating a new row for each professor

##### Extraction Stage

- Extract the `professor_name` column from the `course_professors` column by splitting the values and taking the first part
- Extract the `professor_surname` column from the `course_professors` column by splitting the values and taking the second and remaining
  parts

##### Generation Stage

- Generate the `course_id` column by indexing the column `course_code`
- Generate the `course_professor_id` column by indexing the column `course_professors` flattened

#### Storing Stage

- Store the cleaned data in CSV files with the following columns:

1. Courses: `course_id`, `course_code`, `course_name_mk`, `course_name_en`, `course_url`
2. Professors: `professor_id`, `professor_name`, `professor_surname`
3. Course-Professor: `course_id`, `professor_id`

#### Curriculum-Prerequisite:

##### Loading Stage

- Load the merged data (output from the scraper)

##### Cleaning Stage

- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple whitespaces
- Clean the `study_program_name` and `course_name_mk` columns by removing any leading or trailing whitespaces, as well as occurrences of
  multiple whitespaces, and converting the text to sentence case
- Clean the `course_prerequisite`  column by replacing newline characters with commas, removing any leading or
  trailing whitespaces, as well as occurrences of multiple whitespaces and replacing `nulls` with `нема`.
  Then concatenate the processed values with the pipe(`|`) separator.

#### Merging Stage

- Merge with the course data on `course_code` and `course_name_mk` columns (from the processed course-professor data)
- Merge with the study program data on `study_program_name` and `study_program_duration` columns (from the processed study program data)

##### Extraction Stage

- Extract the `course_level` column from the `course_code` column. The `course_level` is the 4th character of the `course_code`
- Extract the `course_semester_season` column from the `course_semester` column. The `course_semester_season` is calculated based on the
  `course_semester` column such that if the `course_semester` is odd, then `course_semester_season` is `WINTER`,
  otherwise `course_semester_season` is `SUMMER`
- Extract the `course_academic_year` column from the `course_semester` column. The `course_academic_year` is calculated based on the
  `course_semester` as round up of the `course_semester` divided by 2
- Extract the `course_prerequisite_type` column from the `course_prerequisite` column. The `course_prerequisite_type` is determined based on
  the `course_prerequisite` column such that if the `course_prerequisite` column is `немаѝ, then the `course_prerequisite_type` is `NONE`,
  if the `course_prerequisite` column contains any of the following terms `ЕКТС` or `кредити`, then the `course_prerequisite_type` is `
  TOTAL`,
  if the `course_prerequisite` column contains the term `или`, then the `course_prerequisite_type` is `ANY`,
  else the `course_prerequisite_type` is `ONE`
- Extract the `minimum_required_number_of_courses` column from the columns `course_prerequisites` and `course_prerequisite_type`. Calculate
  the
  minimum number of subjects that need to be passed in order to enroll in the course based on matching the digits in
  the `course_prerequisites` divided by the ECTS credits per course (6).

##### Transformation Stage

- Transform the `course_prerequisite` column by splitting the values and validating the course names per study program and calculating the
  minimum number
  of subjects that need to be passed in order to enroll in the course. Remove the courses and their prerequisites that are not valid for the
  study program

#### Flattening Stage

- Flatten the `course_prerequisite` column by splitting the values and creating a new row for each prerequisite
  if `course_prerequisite_type` is `ANY`

##### Generation Stage

- Generate the `course_prerequisites_course_id` by joining the `course_id` and `course_prerequisites` columns

### Results:

This ETL application will produce the following datasets:

1. Study Programs: `study_program_id`, `study_program_code`, `study_program_name`, `study_program_duration`, `study_program_url`
2. Courses: `course_id`, `course_code`, `course_name_mk`, `course_name_en`, `course_url`
3. Professors: `professor_id`, `professor_name`, `professor_surname`
4. Course-Professor: `course_id`, `professor_id`
5. Curricula: `study_program_id`, `course_id`, `course_type`, `course_level`, `course_semester`, `course_semester_season`, `course_academic_year`,
6. Course-Prerequisite: `course_id`, `course_prerequisite_type`, `course_prerequisites_course_id`, `minimum_required_number_of_courses`

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the scraper, make sure to set the following environment variables:

- `FILE_STORAGE_TYPE`: the type of storage to use (either `LOCAL` or `MINIO`)
- `MAX_WORKERS`: the number of workers to use for parallel processing
- `STUDY_PROGRAMS_INPUT_DATA_FILE_PATH`: the path to the study programs data file
- `CURRICULA_INPUT_DATA_FILE_PATH`: the path to the curricula data file
- `COURSE_INPUT_DATA_FILE_PATH`: the path to the courses data file
- `STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME`: the name of the study programs output file
- `COURSES_DATA_OUTPUT_FILE_NAME`: the name of the courses output file
- `PROFESSORS_DATA_OUTPUT_FILE_NAME`: the name of the professors output file
- `CURRICULA_DATA_OUTPUT_FILE_NAME`: the name of the study program-course output file
- `TAUGHT_BY_DATA_OUTPUT_FILE_NAME`: the name of the course-professor output file
- `PREREQUISITES_DATA_OUTPUT_FILE_NAME`: the name of the course-prerequisite output file

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
