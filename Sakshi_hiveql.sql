-- Databricks notebook source
CREATE DATABASE if not exists PROJECT;

-- COMMAND ----------

USE PROJECT;

-- COMMAND ----------

DROP TABLE IF EXISTS clinicaltrial_2021;
CREATE External TABLE if not exists clinicaltrial_2021(Id STRING, 
              Sponsor STRING, Status STRING, 
              Start STRING, Completion STRING, 
              Type STRING ,Submission STRING, 
              Conditions STRING, Interventions STRING)
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/clinicaltrial_2021.csv",
        delimiter "|");              
SELECT * FROM clinicaltrial_2021
where Id<>'Id';

-- COMMAND ----------

SELECT COUNT(*) TOTAL FROM clinicaltrial_2021 WHERE Id<>'Id'; 

-- COMMAND ----------

SELECT Type, COUNT(Type) AS TOTAL
FROM clinicaltrial_2021
WHERE TYPE IS NOT NULL AND TYPE <>'Type'
GROUP BY clinicaltrial_2021.Type
ORDER BY TOTAL DESC;

-- COMMAND ----------

SELECT NEW, COUNT(NEW) AS TOTAL
FROM clinicaltrial_2021 lateral view explode(split(Conditions,',')) Conditions AS NEW
GROUP BY NEW
HAVING TOTAL<65131
ORDER BY TOTAL DESC
LIMIT 5

-- COMMAND ----------

CREATE External TABLE if not exists mesh
                      (TERM STRING, TREE STRING)
USING CSV
OPTIONS (path "dbfs:/FileStore/tables/mesh.csv",
        header "true");
SELECT * FROM mesh;

-- COMMAND ----------

DROP TABLE IF EXISTS SP_STRING;
CREATE OR REPLACE TABLE SP_STRING(
SELECT TERM,TREE,SUBSTRING(TREE,1,3) AS NEW1
FROM mesh);

-- COMMAND ----------

SELECT * FROM SP_STRING
WHERE TERM <> 'term';

-- COMMAND ----------

DROP TABLE IF EXISTS SP_EX;
CREATE OR REPLACE TABLE SP_EX(SELECT Id,Conditions from clinicaltrial_2021
lateral view explode(split(Conditions,',')) Conditions AS NEW);
SELECT * FROM SP_EX;

-- COMMAND ----------

SELECT SP_STRING.NEW1,COUNT(SP_STRING.NEW1) AS TOTAL 
From SP_STRING
INNER JOIN SP_EX ON SP_EX.Conditions = SP_STRING.TERM
GROUP BY SP_STRING.NEW1
ORDER BY TOTAL DESC
LIMIT 10

-- COMMAND ----------

SELECT SP_STRING.NEW1,COUNT(SP_EX.Conditions) AS TOTAL 
From SP_STRING
INNER JOIN SP_EX ON SP_EX.Conditions = SP_STRING.TERM
GROUP BY SP_STRING.NEW1
ORDER BY TOTAL DESC
LIMIT 10

-- COMMAND ----------

DROP TABLE IF EXISTS PHARMA;
CREATE External TABLE IF NOT EXISTS PHARMA(Company STRING,Parent_Company STRING, 
Penalty_Amount STRING, Subtraction_From_Penalty STRING, 
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING, 
Penalty_Year STRING, Penalty_Date STRING, Offense_Group STRING, 
Primary_Offense STRING, Secondary_Offense STRING, Description STRING,
Level_of_Government STRING, Action_Type STRING, 
Agency STRING, Civil_Criminal STRING, 
Prosecution_Agreement STRING,Court STRING,
Case_ID STRING,Private_Litigation_Case_Title STRING,
Lawsuit_Resolution STRING,Facility_State STRING,
City STRING,Address STRING,Zip STRING,
NAICS_Code STRING,NAICS_Translation STRING,
HQ_Country_of_Parent STRING,HQ_State_of_Parent STRING,
Ownership_Structure STRING,
Parent_Company_Stock_Ticker STRING,
Major_Industry_of_Parent STRING,
Specific_Industry_of_Parent STRING,
Info_Source STRING,
Notes STRING
) USING CSV
OPTIONS (path "dbfs:/FileStore/tables/pharma.csv",
        delimiter ",",
        header "true")
        ;
SELECT * FROM PHARMA
LIMIt 5;

-- COMMAND ----------

DROP TABLE IF EXISTS PHARMAS;
CREATE EXTERNAL TABLE IF NOT EXISTS PHARMAS(Parent_Company STRING)
USING CSV
LOCATION 'dbfs:/FileStore/tables/pharma.csv';
SELECT * FROM PHARMAS

-- COMMAND ----------

DROP TABLE IF EXISTS NEW;
CREATE OR REPLACE TABLE NEW(SELECT Sponsor FROM clinicaltrial_2021);
SELECT * FROM NEW

-- COMMAND ----------

SELECT Sponsor, COUNT(Sponsor) AS Total 
From NEW
Left Join PHARMAS ON NEW.Sponsor = PHARMAS.Parent_company
WHERE Parent_Company IS NULL 
Group By Sponsor 
ORDER BY TOTAL DESC
LIMIT 10

-- COMMAND ----------

DROP TABLE IF EXISTS ST_COM;
CREATE OR REPLACE TABLE ST_COM
(SELECT SPLIT(Completion,' ')[0] as MONTH ,SPLIT(Completion,' ')[1] as YEAR,STATUS
FROM clinicaltrial_2021);
Select * from ST_COM

-- COMMAND ----------

SELECT Month,COUNT(MONTH)AS TOTAL
FROM ST_COM
WHERE YEAR == 2021 AND STATUS == 'Completed'
GROUP BY Month
order by CASE
           Month WHEN 'Jan' THEN '01'
                 WHEN 'Feb' THEN '02'
                 WHEN 'Mar' THEN '03'
                 WHEN 'Apr' THEN '04'
                 WHEN 'May' THEN '05'
                 WHEN 'Jun' THEN '06'
                 WHEN 'Jul' THEN '07'
                 WHEN 'Aug' THEN '08'
                 WHEN 'Sep' THEN '09'
                 WHEN 'Oct' THEN '10'
                 WHEN 'Nov' THEN '11'
                 ELSE '12'
                 END
