# FCSE-Skopje 2023 Undergraduate Study Programs Preprocessor

The preprocessor application is used to transform the study programs and related courses from
the [Faculty of Computer Science and Engineering](https://finki.ukim.mk) at
the [Ss. Cyril and Methodius University in Skopje](https://www.ukim.edu.mk).
which can be found at the following [URL](https://finki.ukim.mk/mk/dodiplomski-studii).

## Prerequisites

- Data from
  the [undergraduate-study-program-scraper](https://github.com/username-gigo-is-not-available/undergraduate-study-programs-scraper)
  is
  required to run this application.

## Overview

### Pipeline:

#### Study Program:

##### Load

- Load the study programs data (output from the scraper) with the following columns:
  `study_program_name`, `study_program_url`, `study_program_duration`

##### Clean

- Clean the `study_program_name` column by removing any leading or trailing whitespaces, as well as occurrences of
  multiple whitespaces

##### Extract

- Extract `study_program_code` from `study_program_url` and `study_program_duration`. The `study_program_code` is the
  second to last part of the `study_program_url` concatenated with the `study_program_duration`

##### Generate

- Generate the `study_program_id` column by hashing `study_program_name` and `study_program_duration`

##### Validate

- Validate the study program data against the schema `study_program.avsc`

##### Store

- Store the cleaned data in a file with the following
  columns: `study_program_id`, `study_program_code`, `study_program_name`, `study_program_duration`, `study_program_url`

#### Course:

##### Load

- Load the courses data (output from the scraper) with the following columns:
  `course_code`, `course_name_mk`, `course_name_en`, `course_url`

##### Clean

- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple
  whitespaces
- Clean the `course_name_en` and `course_name_mk` columns by removing any leading or trailing whitespaces, as well as
  occurrences of multiple whitespaces, and converting the text to sentence case while preserving acronyms
-

##### Extract:

- Extract the `course_level` column from the `course_code` column. The `course_level` is the 4th character of the
  `course_code`

##### Generate

- Generate the `course_id` column by hashing `course_name_mk`

##### Validate

- Validate the course data against the schema `course.avsc`

##### Store

- Store the cleaned data in AVRO files with the following columns:
  `course_id`, `course_code`, `course_name_mk`, `course_name_en`, `course_url`, `course_level`

#### Professor:

##### Load

- Load the courses data (output from the scraper) with the following columns:
  `course_code`,  `course_professors`

##### Clean

- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple
  whitespaces
- Clean the `course_professors` column by replacing newline characters with commas, removing any leading or
  trailing whitespaces, as well as occurrences of multiple whitespaces. Remove titles and degrees from the names
  of the professors, then concatenate the processed values with the pipe(`|`) separator

##### Flatten

- Flatten the `course_professors` column by splitting the values and creating a new row for each professor

##### Extract

- Extract the `professor_name` column from the `course_professors` column by splitting the values and taking the first
  part
- Extract the `professor_surname` column from the `course_professors` column by splitting the values and taking the
  second and remaining parts

##### Merge

- Merge with course data on `course_code` column (from the processed course data)

##### Generate

- Generate the `professor_id` column by hashing the column `course_professors` flattened

##### Validate

- Validate the professor data against the schema `professor.avsc`
- Validate the teaches data against the schema `teaches.avsc`

##### Store

- Store the cleaned data in AVRO files with the following columns:

1. Professors: `professor_id`, `professor_name`, `professor_surname`
2. Teaches: `teaches_id`, `course_id`, `professor_id`

#### Requisites:

##### Load

- Load the courses data (output from the scraper) with the following columns:
  `course_code`, `course_prerequisites`

##### Clean

- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple
  whitespaces
- Clean the `course_prerequisite`  column by replacing newline characters with commas, removing any leading or
  trailing whitespaces, as well as occurrences of multiple whitespaces. Then concatenate the processed values with the
  pipe(`|`) separator.


##### Merge

- Merge with course data on `course_code` column

##### Extract

- Extract the `course_prerequisite_type` column from the `course_prerequisite` column. The `course_prerequisite_type` is
  determined based on the `course_prerequisite` column such that if the `course_prerequisite` column is `нема` or `nan`,
  then the `course_prerequisite_type` is `NONE`,
  if the `course_prerequisite` column contains any of the following terms `ЕКТС` or `кредити`, then the
  `course_prerequisite_type` is `TOTAL`, if the `course_prerequisite` column contains the term
  `или`, then the `course_prerequisite_type` is `ANY`, else the `course_prerequisite_type` is `ONE`
- Extract the `minimum_required_number_of_courses` column from the columns `course_prerequisites` and
  `course_prerequisite_type`. Calculate the minimum number of subjects that need to be passed in order to enroll in
  the course based on matching the digits in the `course_prerequisites` divided by the ECTS credits per course (`6`).
  Default value is `0`.

##### Match

- Transform the `course_prerequisite` column by splitting the values and validating the course names.
  if `course_prerequisite_type` is `NONE`, then `course_prerequisite` is `None`
  if `course_prerequisite_type` is `ONE`, then `course_prerequisite` is the course with the highest similarity ratio
  if `course_prerequisite_type` is `ANY`, then `course_prerequisite` are the courses with the highest similarity ratio
  concatenated with the pipe(`|`) separator
  if `course_prerequisite_type` is `TOTAL` then `course_prerequisite` are the all the courses available concatenated
  with
  the pipe(`|`) separator

##### Flatten

- Flatten the `course_prerequisite` column by splitting the values and creating a new row for each prerequisite
  if `course_prerequisite_type` is `ANY` or `TOTAL`
- Create the `prerequisite_course_id` by self-joining on `course_name_mk` and `course_prerequisites`

##### Generate

- Generate the `requisite_id` by hashing the `course_id`, `prerequisite_course_id` and `course_prerequisite_type`
  columns
- Generate the `requires_id` by hashing the columns `course_id` and `requisite_id`
- Generate the `satisfies_id` by hashing the columns `prerequisite_course_id` and `requisite_id`

##### Validate

- Validate the requisite data against the schema `requisite.avsc`
- Validate the requires data against the schema `requires.avsc`
- Validate the satisfies data against the schema `satisfies.avsc`


##### Store

- Store the cleaned data in AVRO files with the following columns:
1. Requisites: `requisite_id`, `course_prerequisite_type`, `minimum_required_number_of_courses`
2. Requires: `requires_id`, `requisite_id`, `course_id`
3. Satisfies: `satisfies_id`, `requisite_id`, `prerequisite_course_id`

#### Curriculum:

##### Load

- Load the curricula data (output from the scraper) with the following columns:
  `study_program_name`, `study_program_duration`, `course_code`, `course_name_mk`, `course_semester`, `course_type`

##### Clean

- Clean the `course_code` column by removing any leading or trailing whitespaces, as well as occurrences of multiple
  whitespaces
- Clean the `study_program_name` and `course_name_mk` columns by removing any leading or trailing whitespaces, as well
  as occurrences of multiple whitespaces, and converting the text to sentence case while preserving acronyms

##### Merge

- Merge with course data on `course_code` and `course_name_mk` columns 
- Merge with study program data on `study_program_name` and `study_program_duration` columns 

##### Extract

- Extract the `course_semester_season` column from the `course_semester` column. The `course_semester_season` is
  calculated based on the `course_semester` column such that if the `course_semester` is odd, then
  `course_semester_season` is `WINTER`, otherwise `course_semester_season` is `SUMMER`
- Extract the `course_academic_year` column from the `course_semester` column. The `course_academic_year` is calculated
  based on the `course_semester` as round up of the `course_semester` divided by 2

##### Generate

- Generate the `curriculum_id` by hashing the following columns `study_program_id`, `course_id`, `course_type`,
  `course_semester`, `course_academic_year`, `course_semester_season`
- Generate the `offers_id` by hashing the columns `study_program_id` and `curriculum_id`
- Generate the `includes_id` by hashing the columns `course_id` and `curriculum_id`

##### Merge (Invalidate offers, depth = 1)

- Merge left with requisites data on `course_id` column
- Self merge left with selected columns `study_program_id`, `course_id` left, 
 left_on `study_program_id`, `prerequisite_course_id`, right_on `study_program_id`, `course_id`,
 prefix the right dataframe with `prerequisite_`

##### Filter (Invalidate offers, depth = 1)

- If `course_prerequisite_type` is `ONE`, then remove if the required course is not offered (`prerequisite_study_program_id` is `None`).
- If `course_prerequisite_type` is `ANY`, then group by (`study_program_id`, `course_id_parent`) and remove the row for the prerequisite that is not
  offered (`prerequisite_study_program_id` is `None` for the whole group).
- If `course_prerequisite_type` is `TOTAL`, then group by (`study_program_id`, `course_id_parent`) and remove if the count of offered prerequisites for the group
  is below the specified threshold (`minimum_required_number_of_courses`).

##### Select

- Select only the relevant columns for invalidating curricula: 
  `study_program_id`, `course_id`, `curiculum_id`, `course_type`, `course_semester`, `course_semester_season`,
  `course_academic_year`

##### Merge (Invalidate offers, depth = 2)

- Merge left with requisites data on `course_id` column
- Self merge left with selected columns `study_program_id`, `course_id` left, 
 left_on `study_program_id`, `prerequisite_course_id`, right_on `study_program_id`, `course_id`,
 prefix the right dataframe with `prerequisite_`

##### Filter (Invalidate offers, depth = 2)

- If `course_prerequisite_type` is `ONE`, then remove if the required course is not offered (`prerequisite_study_program_id` is `None`).
- If `course_prerequisite_type` is `ANY`, then group by (`study_program_id`, `course_id_parent`) and remove the row for the prerequisite that is not
  offered (`prerequisite_study_program_id` is `None` for the whole group).
- If `course_prerequisite_type` is `TOTAL`, then group by (`study_program_id`, `course_id_parent`) and remove if the count of offered prerequisites for the group
  is below the specified threshold (`minimum_required_number_of_courses`).

##### Validate

- Validate the curricula data against the schema `curriculum.avsc`
- Validate the offers data against the schema `offers.avsc`
- Validate the includes data against the schema `includes.avsc`

##### Store

- Store the cleaned data in AVRO files with the following columns:

1. Curricula: `curiculum_id`, `course_type`, `course_semester`, `course_semester_season`, `course_academic_year`
2. Offers: `offers_id`, `curriculum_id`, `study_program_id`
3. Includes: `includes_id`, `curriculum_id`, `course_id`


### Results:

This preprocessor will produce the following datasets:

1. Study Programs: `study_program_id`, `study_program_code`, `study_program_name`, `study_program_duration`, `study_program_url`
2. Courses: `course_id`, `course_code`, `course_name_mk`, `course_name_en`, `course_url`, `course_level`
3. Professors: `professor_id`, `professor_name`, `professor_surname`
4. Teaches: `teaches_id`, `course_id`, `professor_id`
5. Curricula: `curiculum_id`, `course_type`, `course_semester`, `course_semester_season`, `course_academic_year`
6. Offers: `offers_id`, `curriculum_id`, `study_program_id`
7. Includes: `includes_id`, `curriculum_id`, `course_id`
8. Requisites: `requisite_id`, `course_prerequisite_type`, `minimum_required_number_of_courses`
9. Requires: `requires_id`, `requisite_id`, `course_id`
10. Satisfies: `satisfies_id`, `requisite_id`, `prerequisite_course_id`

## Requirements

- Python 3.9 or later

## Environment Variables

Before running the application, make sure to set the following environment variables:

- `FILE_STORAGE_TYPE`: the type of storage to use (either `LOCAL` or `MINIO`)
- `STUDY_PROGRAMS_DATA_INPUT_FILE_PATH`: the path to the study programs data file
- `CURRICULA_DATA_INPUT_FILE_PATH`: the path to the curricula data file
- `COURSE_DATA_INPUT_FILE_PATH`: the path to the courses data file

- `STUDY_PROGRAMS_DATA_OUTPUT_FILE_NAME`: the name of the study programs output file
- `CURRICULA_DATA_OUTPUT_FILE_NAME`: the name of the curricula output file
- `COURSES_DATA_OUTPUT_FILE_NAME`: the name of the courses output file
- `REQUISITES_DATA_OUTPUT_FILE_NAME`: the name of the requisites output file
- `PROFESSORS_DATA_OUTPUT_FILE_NAME`: the name of the professors output file
- `OFFERS_DATA_OUTPUT_FILE_NAME`: the name of the offers output file
- `INCLUDES_DATA_OUTPUT_FILE_NAME`: the name of the includes output file
- `REQUIRES_DATA_OUTPUT_FILE_NAME`: the name of the requires output file
- `SATISFIES_DATA_OUTPUT_FILE_NAME`: the name of the satisfies output file
- `TEACHES_DATA_OUTPUT_FILE_NAME`: the name of the teaches output file

- `STUDY_PROGRAMS_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `StudyProgram` record is stored
- `CURRICULA_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Curriculum` record is stored
- `COURSES_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Course` record is stored
- `REQUISITES_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Requisite` record is stored
- `PROFESSORS_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Professor` record is stored
- `OFFERS_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Offers` record is stored
- `INCLUDES_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Includes` record is stored
- `REQUIRES_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Requires` record is stored
- `SATISFIES_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Satisfies` record is stored
- `TEACHES_SCHEMA_FILE_NAME`: the name of the file where the avro schema for the `Teaches` record is stored

#### If running the application with local storage:

- `INPUT_DATA_DIRECTORY_PATH`: the path to the directory where the input files are stored
- `OUTPUT_DATA_DIRECTORY_PATH`: the path to the directory where the output files will be saved

#### If running the application with MinIO:

- `MINIO_ENDPOINT_URL`: the endpoint of the MinIO server
- `MINIO_ACCESS_KEY`: the access key of the MinIO server
- `MINIO_SECRET_KEY`: the secret key of the MinIO server
- `MINIO_INPUT_DATA_BUCKET_NAME`: the name of the bucket where the input files are stored
- `MINIO_OUTPUT_DATA_BUCKET_NAME`: the name of the bucket where the output data files will be saved
- `MINIO_SCHEMA_BUCKET_NAME`: the name of the bucket where the schema files are stored


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
