### Project: Edutech Data 


**Students Table:**
| StudentID | StudentName | EnrollmentDate |
|-----------|-------------|----------------|
| 1         | Arun        | 2023-09-01     |

**Courses Table:**
| CourseID | CourseName       | Department |
|----------|------------------|------------|
| DADS101  | Data Science     | DADS       |

**Grades Table:**
| GradeID | StudentID | CourseID | Grade | GradeDate   |
|---------|-----------|----------|-------|-------------|
| 1       | 1         | 101      | 85    | 2023-12-01  |

#### Target Database: `DataWarehouse`
We'll create the following tables in the data warehouse:

**FactGrades Table:**
| GradeID | StudentID | CourseID | Grade | GradeDate   |
|---------|-----------|----------|-------|-------------|

**DimStudents Table:**
| StudentID | StudentName | EnrollmentDate |
|-----------|-------------|----------------|

**DimCourses Table:**
| CourseID | CourseName       | Department |
|----------|------------------|------------|

### ETL Process

#### 1. Extract Data
Extract data from the source tables.

-- Extract data from Students table
SELECT * FROM OperationalDB.Students;

-- Extract data from Courses table
SELECT * FROM OperationalDB.Courses;

-- Extract data from Grades table
SELECT * FROM OperationalDB.Grades;

#### 2. Transform Data
Transform the data by cleaning, aggregating, and joining.

**Transform Grades Data:**
Join `Grades` with `Students` and `Courses` to get student and course details.

WITH GradesData AS (
    SELECT 
        g.GradeID,
        g.StudentID,
        s.StudentName,
        s.EnrollmentDate,
        g.CourseID,
        c.CourseName,
        c.Department,
        g.Grade,
        g.GradeDate
    FROM OperationalDB.Grades g
    JOIN OperationalDB.Students s ON g.StudentID = s.StudentID
    JOIN OperationalDB.Courses c ON g.CourseID = c.CourseID
)
SELECT * FROM GradesData;

**Transform Students Data:**
Ensure student names are consistent.

```sql
SELECT 
    StudentID,
    UPPER(StudentName) AS StudentName,
    EnrollmentDate
FROM OperationalDB.Students;
```

**Transform Courses Data:**
Standardize course names and departments.

```sql
SELECT 
    CourseID,
    UPPER(CourseName) AS CourseName,
    UPPER(Department) AS Department
FROM OperationalDB.Courses;
```

#### 3. Load Data
Load the transformed data into the target tables in the data warehouse.

**Load FactGrades Table:**
```sql
INSERT INTO DataWarehouse.FactGrades (GradeID, StudentID, CourseID, Grade, GradeDate)
SELECT 
    GradeID,
    StudentID,
    CourseID,
    Grade,
    GradeDate
FROM GradesData;
```

**Load DimStudents Table:**
```sql
INSERT INTO DataWarehouse.DimStudents (StudentID, StudentName, EnrollmentDate)
SELECT 
    StudentID,
    StudentName,
    EnrollmentDate
FROM OperationalDB.Students;
```

**Load DimCourses Table:**
```sql
INSERT INTO DataWarehouse.DimCourses (CourseID, CourseName, Department)
SELECT 
    CourseID,
    CourseName,
    Department
FROM OperationalDB.Courses;
```

### Complex Queries for Analysis

#### 1. Total Grades by Course
Calculate the total grades for each course.

```sql
SELECT 
    CourseID, 
    SUM(Grade) AS TotalGrades
FROM DataWarehouse.FactGrades
GROUP BY CourseID;
```

#### 2. Top 3 Students by Course
Identify the top 3 students in each course based on their grades.

```sql
WITH RankedGrades AS (
    SELECT
        StudentID,
        CourseID,
        SUM(Grade) AS TotalGrades,
        DENSE_RANK() OVER (PARTITION BY CourseID ORDER BY SUM(Grade) DESC) AS dense_rank
    FROM DataWarehouse.FactGrades
    GROUP BY StudentID, CourseID
)
SELECT
    StudentID,
    CourseID,
    TotalGrades
FROM RankedGrades
WHERE dense_rank <= 3;
```

#### 3. Monthly Grade Trend
Analyze the monthly grade trend for the year 2023.

```sql
SELECT 
    DATE_TRUNC('month', GradeDate) AS Month, 
    SUM(Grade) AS TotalGrades
FROM DataWarehouse.FactGrades
WHERE EXTRACT(YEAR FROM GradeDate) = 2023
GROUP BY DATE_TRUNC('month', GradeDate)
ORDER BY Month;
```

#### 4. Grades by Department
Calculate the total grades for each department.

```sql
SELECT 
    c.Department, 
    SUM(f.Grade) AS TotalGrades
FROM DataWarehouse.FactGrades f
JOIN DataWarehouse.DimCourses c ON f.CourseID = c.CourseID
GROUP BY c.Department;
```

#### 5. Grade Ranking by Course
Rank the students based on their total grades, partitioned by course.

```sql
SELECT 
    StudentID, 
    CourseID, 
    SUM(Grade) AS TotalGrades,
    RANK() OVER (PARTITION BY CourseID ORDER BY SUM(Grade) DESC) AS rank
FROM DataWarehouse.FactGrades
GROUP BY StudentID, CourseID;
```
