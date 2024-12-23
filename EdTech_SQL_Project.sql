--- Project: Edutech Data 

--- ETL Process

--- 1. Extract Data
--- Extract data from the source tables.

-- Extract data from Students table
SELECT * FROM OperationalDB.Students;

-- Extract data from Courses table
SELECT * FROM OperationalDB.Courses;

-- Extract data from Grades table
SELECT * FROM OperationalDB.Grades;

---  2. Transform Data
--- Transform the data by cleaning, aggregating, and joining.

--- **Transform Grades Data:**

--- Join Grades with Students and Courses to get student and course details.

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
    
--- **Transform Students Data:**
--- Ensure student names are consistent.


SELECT 
    StudentID,
    UPPER(StudentName) AS StudentName,
    EnrollmentDate
FROM OperationalDB.Students;

--- **Transform Courses Data:**
Standardize course names and departments.

SELECT 
    CourseID,
    UPPER(CourseName) AS CourseName,
    UPPER(Department) AS Department
FROM OperationalDB.Courses;


--- 3. Load Data
--- Load the transformed data into the target tables in the data warehouse.

--- **Load FactGrades Table:**

    INSERT INTO DataWarehouse.FactGrades (GradeID, StudentID, CourseID, Grade, GradeDate)
SELECT 
    GradeID,
    StudentID,
    CourseID,
    Grade,
    GradeDate
FROM GradesData;

--- **Load DimStudents Table:**

INSERT INTO DataWarehouse.DimStudents (StudentID, StudentName, EnrollmentDate)
SELECT 
    StudentID,
    StudentName,
    EnrollmentDate
FROM OperationalDB.Students;

--- **Load DimCourses Table:**

    INSERT INTO DataWarehouse.DimCourses (CourseID, CourseName, Department)
SELECT 
    CourseID,
    CourseName,
    Department
FROM OperationalDB.Courses;

--- Complex Queries for Analysis

--- 1. Calculate the total grades for each course.


SELECT 
    CourseID, 
    SUM(Grade) AS TotalGrades
FROM DataWarehouse.FactGrades
GROUP BY CourseID;

--- 2. Identify the top 3 students in each course based on their grades.


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

--- 3.Analyze the monthly grade trend for the year 2023.

SELECT 
    DATE_TRUNC('month', GradeDate) AS Month, 
    SUM(Grade) AS TotalGrades
FROM DataWarehouse.FactGrades
WHERE EXTRACT(YEAR FROM GradeDate) = 2023
GROUP BY DATE_TRUNC('month', GradeDate)
ORDER BY Month;


--- 4.Calculate the total grades for each department.

SELECT 
    c.Department, 
    SUM(f.Grade) AS TotalGrades
FROM DataWarehouse.FactGrades f
JOIN DataWarehouse.DimCourses c ON f.CourseID = c.CourseID
GROUP BY c.Department;

--- 5.Rank the students based on their total grades, partitioned by course.

SELECT 
    StudentID, 
    CourseID, 
    SUM(Grade) AS TotalGrades,
    RANK() OVER (PARTITION BY CourseID ORDER BY SUM(Grade) DESC) AS rank
FROM DataWarehouse.FactGrades
GROUP BY StudentID, CourseID;
