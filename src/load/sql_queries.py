# DROP TABLES

dim_source_table_drop = "DROP TABLE IF EXISTS dim_source;"
dim_date_table_drop = "DROP TABLE IF EXISTS dim_date;"
dim_project_table_drop = "DROP TABLE IF EXISTS dim_project;"
dim_bucket_table_drop = "DROP TABLE IF EXISTS dim_bucket;"
dim_member_table_drop = "DROP TABLE IF EXISTS dim_member;"
dim_task_allocation_table_drop = "DROP TABLE IF EXISTS dim_task_allocation;"
fact_task_table_drop = "DROP TABLE IF EXISTS fact_task;"

# CREATE TABLES

dim_source_table_create = ("""
IF OBJECT_ID('dbo.dim_source') IS NULL CREATE TABLE dim_source \
    (Id int NOT NULL IDENTITY(1,1), \
    Name varchar(20) NULL);
""")

dim_date_table_create = ("""
IF OBJECT_ID('dbo.dim_date') IS NULL CREATE TABLE dim_date \
    (Date datetime2 NOT NULL, \
    Day int NOT NULL, \
    Day_of_week int NOT NULL, \
    Week int NULL, \
    Month int NOT NULL, \
    Year int NOT NULL, \
    Is_working_day varchar(10) NOT NULL);
""")

dim_project_table_create = ("""
IF OBJECT_ID('dbo.dim_project') IS NULL CREATE TABLE dim_project \
    (Id varchar(50) NOT NULL, \
    Name nvarchar(100) NULL, \
    Created_Date datetime2 NULL);
""")

dim_bucket_table_create = ("""
IF OBJECT_ID('dbo.dim_bucket') IS NULL CREATE TABLE dim_bucket \
    (Id varchar(50) NOT NULL, \
    Name nvarchar(50) NULL, \
    Project_Id varchar(50) NULL);
""")

dim_member_table_create = ("""
IF OBJECT_ID('dbo.dim_member') IS NULL CREATE TABLE dim_member \
    (Account_Id varchar(50) NOT NULL, \
    Full_Name nvarchar(50) NULL, \
    User_Name varchar(50) NULL, \
    Source_Id int NULL);
""")

dim_task_allocation_table_create = ("""
IF OBJECT_ID('dbo.dim_task_allocation') IS NULL CREATE TABLE dim_task_allocation \
    (Id int NOT NULL IDENTITY(1,1), \
    Task_Id varchar(50) NULL, \
    Account_Id varchar(50) NULL);
""")

fact_task_table_create = ("""
IF OBJECT_ID('dbo.fact_task') IS NULL CREATE TABLE fact_task \
    (Id varchar(50) NOT NULL, \
    Name nvarchar(200) NULL, \
    Description ntext NULL, \
    Created_Date datetime2 NULL, \
    Date_Last_Activity datetime2 NULL, \
    Start_Date datetime2 NULL, \
    End_Date datetime2 NULL, \
    Finished_Date datetime2 NULL, \
    Bucket_Id varchar(50) NULL, \
    Project_Id varchar(50) NULL, \
    Source_Id varchar(50) NULL);
""")

dim_source_insert = ("""
INSERT INTO dbo.dim_source (Name)
VALUES (?);
""")

dim_date_insert = ("""
INSERT INTO dbo.dim_date (Date, Day, Day_of_week, \
                    Week, Month, Year, \
                    Is_working_day)
VALUES (?, ?, ?, ?, ?, ?, ?)
""")

dim_project_insert = ("""
INSERT INTO dbo.dim_project (Id, Name, Created_Date)
VALUES (?, ?, ?)
""")

dim_bucket_insert = ("""
INSERT INTO dbo.dim_bucket (Id, Name, Project_Id)
VALUES (?, ?, ?)
""")

dim_member_insert = ("""
INSERT INTO dbo.dim_member (Account_Id, Full_Name, User_Name, Source_Id)
VALUES (?, ?, ?, ?)
""")

dim_task_allocation_insert = ("""
INSERT INTO dbo.dim_task_allocation (Task_Id, Account_Id)
VALUES (?, ?);
""")

fact_task_insert = ("""
INSERT INTO dbo.fact_task (Id, Name, Description, \
                        Created_Date, Date_Last_Activity, \
                        Start_Date, End_Date, Finished_Date, \
                        Bucket_Id, Project_Id, Source_Id)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# QUERY LISTS
create_table_queries = [dim_source_table_create, dim_date_table_create, \
                        dim_project_table_create, dim_bucket_table_create, \
                        dim_member_table_create, dim_task_allocation_table_create, \
                        fact_task_table_create]
drop_table_queries = [dim_source_table_drop, dim_date_table_drop, \
                    dim_project_table_drop, dim_bucket_table_drop, \
                    dim_member_table_drop, dim_task_allocation_table_drop, \
                    fact_task_table_drop]
insert_table_queries = [dim_source_insert, dim_date_insert, \
                    dim_project_insert, dim_bucket_insert, \
                    dim_member_insert, dim_task_allocation_insert, \
                    fact_task_insert]




