# Spark UDF of CMS DRG Grouper v39

MS-DRGs are a summarization of inpatient stay information into a simple 3 digit DRG Code. This is done through a defined set of rules set forth by CMS, and recently CMS has provided source Java code that implements these sets of rules. In addition to the DRG Code, a medical/surgical indicator as well as a major diagnostic category (MDC) code is produced by the grouper software. More information can be found on CMS's [website](https://www.cms.gov/medicare/medicare-fee-for-service-payment/acuteinpatientpps/ms-drg-classifications-and-software)


## Installing on Databricks

Attach the repo jar package to your Spark 3.3.0, scala 2.12 Databricks cluster runtime.

## Running on Databricks

### Input

8 fields are accepted as ordered paramters to the drg grouper. A none existant value should be passed as a blank string e.g. "". 
  1. age
  2. sex
  3. lengthOfStay 
  4. admit diagnosis code
  5. principal diagnosis code
  6. hcfa discharge status code
  7. other diagnosis codes (a list comma sepearted)
  8. other procedure codes (a list comma seperated)

```scala
%scala 

//Register as a UDF
spark.sqlContext.udf.register("drgUDF", new com.databricks.labs.msdrg.DrgUDF().call _)

val df = spark.sql(""" select drgUDF('0', 'M', '0', 'I120', 'I120', '1', 'E0800', '0TY00Z0,0FYG0Z0,5A1D70Z') as drg """)

```

### Output 

### Sample DRG, MDC, and Medical/Surgical indicators

[Index by MDC](https://www.cms.gov/icd10m/version39-fullcode-cms/fullcode_cms/P0001.html)
[Index by DRG](https://www.cms.gov/icd10m/version39-fullcode-cms/fullcode_cms/P0002.html)

