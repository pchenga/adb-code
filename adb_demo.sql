-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set("fs.azure.account.key.azurestorageadlsgen27.dfs.core.windows.net","8L52fd0gS/EiTmhX4fYOgIuj2SScuPgJsfAzLLeQE7TtYtm9MkxBUi4cPpY/MCWyofVHzLEz9/dI+AStZij31g==")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC emp_file_path = 'abfss://csvdata@azurestorageadlsgen27.dfs.core.windows.net/emp_data.csv'
-- MAGIC #dept_file_path = 'abfss://csvdata@azurestorageadlsgen27.dfs.core.windows.net/dept_data.csv'
-- MAGIC 
-- MAGIC #emp_df = spark.read.csv(emp_file_path)
-- MAGIC #dept_df = spark.read.option('header', 'true').csv(dept_file_path)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC 
-- MAGIC data = [(1, 'A'),
-- MAGIC         (2, 'B')
-- MAGIC        ]
-- MAGIC 
-- MAGIC schema = ["id", "name"]
-- MAGIC 
-- MAGIC df = spark.createDataFrame(data = data , schema = schema)
-- MAGIC display(df)

-- COMMAND ----------

select * from emp_data


-- COMMAND ----------

select * from dept_data

-- COMMAND ----------

-- DBTITLE 1,SQL Transfromations
CREATE TABLE emp_dept AS 
select
e.eid as employee_id,
e.ename as employee_name,
e.sal as employee_salary,
d.deptname,
'India' as city
 from emp_data e
left join dept_data d 
on e.deptid = d.deptid

