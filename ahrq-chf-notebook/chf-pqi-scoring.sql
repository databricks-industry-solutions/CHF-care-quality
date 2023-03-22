-- Databricks notebook source
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

CREATE WIDGET TEXT denominator_icd_dx_inclusions DEFAULT ("'39891', '4280', '4281', '42820', '42821', '42822', '42823', '42830', '42831', '42832', '42833', '42840', '42841', '42842', '42843', '4289'");

-- COMMAND ----------

--Morgan, create this table as just denominator only members. Include year so if a member has 3 years of claims they may show up in this table 3x
--DROP TABLE IF EXISTS ...;
--TODO_MORGAN, Include in the claims both out_claims and car_claims as well (outpatient and pcp )
--TODO_MORGAN, output table of the member id and year (these metrics are YoY)
--TODO_MORGAN, exclude members (member/year) who have end stage renal disease ICD-9 code 585.6 
--TODO_???, not eligible for both A & B (check ben_sum file data dictionary)
CREATE TABLE omop_patient_risk.COHORT_GROUP AS 
(select inp.*, 
 case when arrays_overlap(array(${icd9_cols}),array(${cohort_numerator_exclusion})) is true then 0 else 1 end as numerator
from hls_healthcare.hls_cms_synpuf.inp_claims inp
where arrays_overlap(array(${icd9_cols}),array(${cohort_denominator})))

-- COMMAND ----------

CREATE WIDGET TEXT numerator_icd_px_inclusions DEFAULT ("'0050', '0051', '0052', '0053', '0054', '0056', '0057', '0066', '1751', '1752', '1755', '3500', '3501', '3502', '3503', '3504', '3505', '3506', '3507', '3508', '3509', '3510', '3511', '3512', '3513', '3514', '3520', '3521', '3522', '3523', '3524', '3525', '3526', '3527', '3528', '3531', '3532', '3533', '3534', '3535', '3539', '3541', '3542', '3550', '3551', '3552', '3553', '3554', '3555', '3560', '3561', '3562', '3563', '3570', '3571', '3572', '3573', '3581', '3582', '3583', '3584', '3591', '3592', '3593', '3594', '3595', '3596', '3597', '3598', '3599', '3603', '3604', '3606', '3607', '3609', '3610', '3611', '3612', '3613', '3614', '3615', '3616', '3617', '3619', '362', '3631', '3632', '3633', '3634', '3639', '3691', '3699', '3731', '3732', '3733', '3734', '3735', '3736', '3737', '3741', '3751', '3752', '3753', '3754', '3755', '3760', '3761', '3762', '3763', '3764', '3765', '3766', '3770', '3771', '3772', '3773', '3774', '3775', '3776', '3777', '3778', '3779', '3780', '3781', '3782', '3783', '3785', '3786', '3787', '3789', '3794', '3795', '3796', '3797', '3798', '3826'");

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating the Numerator

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Calculating Quality Metric for ACOs
