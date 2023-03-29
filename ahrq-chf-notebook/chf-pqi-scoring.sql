-- Databricks notebook source
-- MAGIC %python
-- MAGIC from functools import reduce
-- MAGIC import py4j

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #CHF Preventative Quality Indicator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating the Denominator 
-- MAGIC 
-- MAGIC Any patient who has been diagnosed with CHF in any setting

-- COMMAND ----------

CREATE WIDGET TEXT SOURCE_DB  DEFAULT "hls_cms_synpuf";
CREATE WIDGET TEXT TARGET_DB  DEFAULT "ahrq_pqi";

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${TARGET_DB};
use ${SOURCE_DB};
show tables

-- COMMAND ----------

-- DBTITLE 1,Create Widget for Denominator Diagnosis Inclusions
CREATE WIDGET TEXT denominator_icd_dx_inclusions DEFAULT ("'39891', '4280', '4281', '42820', '42821', '42822', '42823', '42830', '42831', '42832', '42833', '42840', '42841', '42842', '42843', '4289'");

-- COMMAND ----------

-- DBTITLE 1,Create Widget for Numerator Procedure Exclusions - Missing exclusions in the 3620's!!!
CREATE WIDGET TEXT numerator_icd_px_exclusions DEFAULT ("'0050', '0051', '0052', '0053', '0054', '0056', '0057', '0066', '1751', '1752', '1755', '3500', '3501', '3502', '3503', '3504', '3505', '3506', '3507', '3508', '3509', '3510', '3511', '3512', '3513', '3514', '3520', '3521', '3522', '3523', '3524', '3525', '3526', '3527', '3528', '3531', '3532', '3533', '3534', '3535', '3539', '3541', '3542', '3550', '3551', '3552', '3553', '3554', '3555', '3560', '3561', '3562', '3563', '3570', '3571', '3572', '3573', '3581', '3582', '3583', '3584', '3591', '3592', '3593', '3594', '3595', '3596', '3597', '3598', '3599', '3603', '3604', '3606', '3607', '3609', '3610', '3611', '3612', '3613', '3614', '3615', '3616', '3617', '3619', '3631', '3632', '3633', '3634', '3639', '3691', '3699', '3731', '3732', '3733', '3734', '3735', '3736', '3737', '3741', '3751', '3752', '3753', '3754', '3755', '3760', '3761', '3762', '3763', '3764', '3765', '3766', '3770', '3771', '3772', '3773', '3774', '3775', '3776', '3777', '3778', '3779', '3780', '3781', '3782', '3783', '3785', '3786', '3787', '3789', '3794', '3795', '3796', '3797', '3798', '3826'");
--'362'

-- COMMAND ----------

-- DBTITLE 1,Functions selecting Diagnosis and Procedure columns
-- MAGIC %python
-- MAGIC #dynamically select diagnosis columns
-- MAGIC def dx_cols(tablename):
-- MAGIC   return [str(col.name) for col in spark.table(tablename).schema.fields if 'DGNS' in col.name]
-- MAGIC #dynamically select procedure columns
-- MAGIC def px_cols(tablename):
-- MAGIC   return [str(col.name) for col in spark.table(tablename).schema.fields if ('PRCDR' in col.name or 'HCPCS' in col.name)]

-- COMMAND ----------

-- DBTITLE 1,Widget for Inpatient DX Columns
-- MAGIC %python
-- MAGIC widget_name = 'inp_dx_cols'
-- MAGIC tables = ['hls_healthcare.hls_cms_synpuf.inp_claims']
-- MAGIC columns_select = (', '.join(list(reduce(lambda a, b: a+b, map(dx_cols, tables))))) #diagnosis columns
-- MAGIC try:
-- MAGIC   dbutils.widgets.remove(widget_name)
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)
-- MAGIC except py4j.protocol.Py4JJavaError:
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)  
-- MAGIC finally:
-- MAGIC   print(columns_select)

-- COMMAND ----------

-- DBTITLE 1,Widget for Inpatient PX Columns
-- MAGIC %python
-- MAGIC widget_name = 'inp_px_cols'
-- MAGIC tables = ['hls_healthcare.hls_cms_synpuf.inp_claims']
-- MAGIC columns_select = (', '.join(list(reduce(lambda a, b: a+b, map(px_cols, tables))))) #procedure columns
-- MAGIC try:
-- MAGIC   dbutils.widgets.remove(widget_name)
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)
-- MAGIC except py4j.protocol.Py4JJavaError:
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)  
-- MAGIC finally:
-- MAGIC   print(columns_select)

-- COMMAND ----------

-- DBTITLE 1,Widget for Carrier DX Columns
-- MAGIC %python
-- MAGIC widget_name = 'car_dx_cols'
-- MAGIC tables = ['hls_healthcare.hls_cms_synpuf.car_claims']
-- MAGIC columns_select = (', '.join(list(reduce(lambda a, b: a+b, map(dx_cols, tables))))) #diagnosis columns
-- MAGIC try:
-- MAGIC   dbutils.widgets.remove(widget_name)
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)
-- MAGIC except py4j.protocol.Py4JJavaError:
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)  
-- MAGIC finally:
-- MAGIC   print(columns_select)

-- COMMAND ----------

-- DBTITLE 1,Widget for Carrier PX Columns
-- MAGIC %python
-- MAGIC widget_name = 'car_px_cols'
-- MAGIC tables = ['hls_healthcare.hls_cms_synpuf.car_claims']
-- MAGIC columns_select = (', '.join(list(reduce(lambda a, b: a+b, map(px_cols, tables))))) #procedure columns
-- MAGIC try:
-- MAGIC   dbutils.widgets.remove(widget_name)
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)
-- MAGIC except py4j.protocol.Py4JJavaError:
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)  
-- MAGIC finally:
-- MAGIC   print(columns_select)

-- COMMAND ----------

-- DBTITLE 1,Widget for Outpatient DX Columns
-- MAGIC %python
-- MAGIC widget_name = 'out_dx_cols'
-- MAGIC tables = ['hls_healthcare.hls_cms_synpuf.out_claims']
-- MAGIC columns_select = (', '.join(list(reduce(lambda a, b: a+b, map(dx_cols, tables))))) #diagnosis columns
-- MAGIC try:
-- MAGIC   dbutils.widgets.remove(widget_name)
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)
-- MAGIC except py4j.protocol.Py4JJavaError:
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)  
-- MAGIC finally:
-- MAGIC   print(columns_select)

-- COMMAND ----------

-- DBTITLE 1,Widget for Outpatient PX Columns
-- MAGIC %python
-- MAGIC widget_name = 'out_px_cols'
-- MAGIC tables = ['hls_healthcare.hls_cms_synpuf.out_claims']
-- MAGIC columns_select = (', '.join(list(reduce(lambda a, b: a+b, map(px_cols, tables))))) #procedure columns
-- MAGIC try:
-- MAGIC   dbutils.widgets.remove(widget_name)
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)
-- MAGIC except py4j.protocol.Py4JJavaError:
-- MAGIC   dbutils.widgets.text(widget_name, columns_select)  
-- MAGIC finally:
-- MAGIC   print(columns_select)

-- COMMAND ----------

--Morgan, create this table as just denominator only members. Include year so if a member has 3 years of claims they may show up in this table 3x
--DROP TABLE IF EXISTS ...;
--TODO_MORGAN, Include in the claims both out_claims and car_claims as well (outpatient and pcp)
--TODO_MORGAN, output table of the member id and year (these metrics are YoY)

--TODO_MORGAN, exclude members (member/year) who have end stage renal disease ICD-9 code 585.6 -> 5856 exclude from denominator

--TODO_MORGAN, not eligible for both A & B (check ben_sum file data dictionary)


CREATE TABLE omop_patient_risk.COHORT_GROUP AS 
(select inp.*, 
 case when arrays_overlap(array(${icd9_cols}),array(${cohort_numerator_exclusion})) is true then 0 else 1 end as numerator
from hls_healthcare.hls_cms_synpuf.inp_claims inp
where arrays_overlap(array(${icd9_cols}),array(${cohort_denominator})))

-- COMMAND ----------

-- DBTITLE 1,Use Target Database/Schema for Creating Tables
USE ${TARGET_DB}

-- COMMAND ----------

-- DBTITLE 1,Using Left ANTI JOIN -- slightly less performant than where not exists
CREATE OR REPLACE TABLE COHORT_INP AS 
(select a.DESYNPUF_ID, left(a.CLM_ADMSN_DT,4) as admission_year

from ${SOURCE_DB}.inp_claims a

anti join (select DESYNPUF_ID, left(CLM_ADMSN_DT,4) as admission_year
from ${SOURCE_DB}.inp_claims
where arrays_overlap(array(${inp_dx_cols}),array('5856'))
group by 1,2)b
on a.DESYNPUF_ID = b.DESYNPUF_ID
and left(a.CLM_ADMSN_DT,4) = b.admission_year

where (arrays_overlap(array(${inp_dx_cols}),array(${denominator_icd_dx_inclusions})))
group by 1,2)

-- COMMAND ----------

-- DBTITLE 1,Create Denominator for Inpatient Table, grouped by ID & YEAR. Excluding ESRD 5856
CREATE OR REPLACE TABLE COHORT_INP AS 
(select DESYNPUF_ID, left(CLM_ADMSN_DT,4) as admission_year

from ${SOURCE_DB}.inp_claims a

where arrays_overlap(array(${inp_dx_cols}),array(${denominator_icd_dx_inclusions}))
and not exists (select 1
from ${SOURCE_DB}.inp_claims b
where arrays_overlap(array(${inp_dx_cols}),array('5856'))
and a.DESYNPUF_ID = b.DESYNPUF_ID
and left(a.CLM_ADMSN_DT,4) = left(b.CLM_ADMSN_DT,4))

group by 1,2)

-- COMMAND ----------

-- DBTITLE 1,Create Denominator for Carrier Table, grouped by ID & YEAR. Excluding ESRD 5856
CREATE OR REPLACE TABLE COHORT_CAR AS 
(select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year

from ${SOURCE_DB}.car_claims a

where arrays_overlap(array(${car_dx_cols}),array(${denominator_icd_dx_inclusions}))
and not exists (select 1
from ${SOURCE_DB}.car_claims b
where arrays_overlap(array(${car_dx_cols}),array('5856'))
and a.DESYNPUF_ID = b.DESYNPUF_ID
and left(a.CLM_FROM_DT,4) = left(b.CLM_FROM_DT,4))

group by 1,2)

-- COMMAND ----------

-- DBTITLE 1,Create Denominator for OUT Patient Table, grouped by ID & YEAR. Excluding ESRD 5856
CREATE OR REPLACE TABLE COHORT_OUT AS 
(select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year

from ${SOURCE_DB}.out_claims a

where arrays_overlap(array(${out_dx_cols}),array(${denominator_icd_dx_inclusions}))
and not exists (select 1
from ${SOURCE_DB}.out_claims b
where arrays_overlap(array(${out_dx_cols}),array('5856'))
and a.DESYNPUF_ID = b.DESYNPUF_ID
and left(a.CLM_FROM_DT,4) = left(b.CLM_FROM_DT,4))

group by 1,2)

-- COMMAND ----------

-- DBTITLE 1,CREATING COHORT_DENOM via UNION of INP + CAR + OUT. Excluding ESRD 5856. Requiring Medicare Coverage
CREATE OR REPLACE TABLE COHORT_DENOM AS 
select c.DESYNPUF_ID, c.admission_year
from ((select DESYNPUF_ID, left(CLM_ADMSN_DT,4) as admission_year
from ${SOURCE_DB}.inp_claims a
where arrays_overlap(array(${inp_dx_cols}),array(${denominator_icd_dx_inclusions}))
and not exists (select 1
from ${SOURCE_DB}.inp_claims b
where arrays_overlap(array(${inp_dx_cols}),array('5856'))
and a.DESYNPUF_ID = b.DESYNPUF_ID
and left(a.CLM_ADMSN_DT,4) = left(b.CLM_ADMSN_DT,4)))
UNION DISTINCT
(select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year
from ${SOURCE_DB}.car_claims a
where arrays_overlap(array(${car_dx_cols}),array(${denominator_icd_dx_inclusions}))
and not exists (select 1
from ${SOURCE_DB}.car_claims b
where arrays_overlap(array(${car_dx_cols}),array('5856'))
and a.DESYNPUF_ID = b.DESYNPUF_ID
and left(a.CLM_FROM_DT,4) = left(b.CLM_FROM_DT,4)))
UNION DISTINCT
(select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year
from ${SOURCE_DB}.out_claims a
where arrays_overlap(array(${out_dx_cols}),array(${denominator_icd_dx_inclusions}))
and not exists (select 1
from ${SOURCE_DB}.out_claims b
where arrays_overlap(array(${out_dx_cols}),array('5856'))
and a.DESYNPUF_ID = b.DESYNPUF_ID
and left(a.CLM_FROM_DT,4) = left(b.CLM_FROM_DT,4))))c
inner join (select DESYNPUF_ID
from ${SOURCE_DB}.ben_sum
where BENE_HI_CVRAGE_TOT_MONS > 0
and BENE_SMI_CVRAGE_TOT_MONS > 0
group by 1)d
on c.DESYNPUF_ID = d.DESYNPUF_ID

-- COMMAND ----------

-- DBTITLE 1,Alternatively, create BENE table then inner join to COHORT_DENOM
CREATE OR REPLACE TABLE BENE_COV AS
(select DESYNPUF_ID
from ${SOURCE_DB}.ben_sum
where BENE_HI_CVRAGE_TOT_MONS > 0
and BENE_SMI_CVRAGE_TOT_MONS > 0
group by 1)

-- COMMAND ----------

-- DBTITLE 1,Alternative, joining BENE to COHORT_DENOM
CREATE OR REPLACE TABLE COHORT_DENOM AS 
(select a.DESYNPUF_ID, a.admission_year

from COHORT_DENOM a
inner join BENE_COV b
on a.DESYNPUF_ID = b.DESYNPUF_ID
)

-- COMMAND ----------



-- COMMAND ----------

select count(*) from COHORT_DENOM

-- COMMAND ----------

select count(*) from COHORT_DENOM_FINALselect count(*) from COHORT_DENOM_FINAL

-- COMMAND ----------

select count(*) from COHORT_DENOM

-- COMMAND ----------

 SELECT count(*) from BENE_COV

-- COMMAND ----------

-- DBTITLE 1,Create Denominator for Inpatient Table, grouped by ID & YEAR
CREATE OR REPLACE TABLE COHORT_INP AS 
(select DESYNPUF_ID, left(CLM_ADMSN_DT,4) as admission_year
from ${SOURCE_DB}.inp_claims inp
where arrays_overlap(array(${inp_dx_cols}),array(${denominator_icd_dx_inclusions}))
group by 1,2)

-- COMMAND ----------

-- DBTITLE 1,Create Denominator for Carrier Table, grouped by ID & YEAR
CREATE OR REPLACE TABLE COHORT_CAR AS 
(select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year
from ${SOURCE_DB}.car_claims
where arrays_overlap(array(${car_dx_cols}),array(${denominator_icd_dx_inclusions}))
group by 1,2)

-- COMMAND ----------

-- DBTITLE 1,Create Denominator for Outpatient Table, grouped by ID & YEAR
CREATE OR REPLACE TABLE COHORT_OUT AS 
(select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year
from ${SOURCE_DB}.out_claims
where arrays_overlap(array(${out_dx_cols}),array(${denominator_icd_dx_inclusions}))
group by 1,2)

-- COMMAND ----------

-- DBTITLE 1,Create Denominator using INP, CAR, OUT Tables, grouped by ID & YEAR
CREATE OR REPLACE TABLE COHORT_DENOM AS 
(select DESYNPUF_ID, left(CLM_ADMSN_DT,4) as admission_year
from ${SOURCE_DB}.inp_claims
where arrays_overlap(array(${inp_dx_cols}),array(${denominator_icd_dx_inclusions}))
)
UNION DISTINCT
(select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year
from ${SOURCE_DB}.car_claims
where arrays_overlap(array(${car_dx_cols}),array(${denominator_icd_dx_inclusions}))
)
UNION DISTINCT
(select DESYNPUF_ID, left(CLM_FROM_DT,4) as admission_year
from ${SOURCE_DB}.out_claims
where arrays_overlap(array(${out_dx_cols}),array(${denominator_icd_dx_inclusions}))
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #dbutils.widgets.remove("inp_icd9_dx_cols")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating the Numerator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating Quality Metric for ACOs
