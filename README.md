# SQL_Projects
Collection of my SQL projects and Queries

# Edutech Data Warehouse Project

## Project Overview

This project demonstrates an end-to-end ETL process and complex queries for an edutech data warehouse. It includes extracting data from source tables, transforming the data, and loading it into target tables, followed by various complex analytical queries.

## Data Source

### Operational Database (`OperationalDB`)
- **Students Table:**
  | StudentID | StudentName | EnrollmentDate |
  |-----------|-------------|----------------|
  | 1         | Alice       | 2023-09-01     |

- **Courses Table:**
  | CourseID | CourseName       | Department |
  |----------|------------------|------------|
  | 101      | Data Science     | Computer   |

- **Grades Table:**
  | GradeID | StudentID | CourseID | Grade | GradeDate   |
  |---------|-----------|----------|-------|-------------|
  | 1       | 1         | 101      | 85    | 2023-12-01  |
  | 2       | 2         | 102      | 90    | 2023-12-01  |
  | 3       | 3         | 103      | 88    | 2023-12-01  |

## Target Database (`DataWarehouse`)

- **FactGrades Table:**
  | GradeID | StudentID | CourseID | Grade | GradeDate   |
  |---------|-----------|----------|-------|-------------|


- **DimStudents Table:**
  | StudentID | StudentName | EnrollmentDate |
  |-----------|-------------|----------------|


- **DimCourses Table:**
  | CourseID | CourseName       | Department |
  |----------|------------------|------------|
