![image](https://raw.githubusercontent.com/databricks-industry-solutions/.github/main/profile/solacc_logo_wide.png)

[![CLOUD](https://img.shields.io/badge/CLOUD-ALL-blue?logo=googlecloud&style=for-the-badge)](https://cloud.google.com/databricks)
[![POC](https://img.shields.io/badge/POC-10_days-green?style=for-the-badge)](https://databricks.com/try-databricks)

## Congestive Heart Failure Quality Metrics & MS-DRG as a SQL UDF 

Patient Risk and Quality are closely intertwined and as a follow up to [predicting patient risk](https://github.com/databricks-industry-solutions/hls-patient-risk)we determine which organizations are best suited to treat at risk patients for Congestive Heart Failure. 

The measure developed is a value between 0 (good) and 1 (bad) for providers who treat patients with CHF. This measure is part of the Preventative Quality Indicators (PQIs) developed by the Agency for Healthcare Research and Quality (AHRQ).
___

test edit
TODO IMAGE TO REFERENCE ARCHITECTURE for Patient Risk

___

## Getting started

### Measuring Quality with PQI Indicators 

PQI indicators are typically assigned to an Affordable Care Organization (ACO) and show an organiation's effectiveness at preventative treatments. For this measure we are determining which organizations have the best score (closest to 0) to treat Congestive Heart Failure. 

TODO Input -> Process -> Output 

### Calculating MS-DRGs from a UDF for measurement inputs

MS-DRGs are a representation of inpatient claims for payment adjustments. 

TODO Input -> Process -> Output

### Libraries 

&copy; 2022 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

| library                                | description             | license    | source                                              |
|----------------------------------------|-------------------------|------------|-----------------------------------------------------|
| PyYAML                                 | Reading Yaml files      | MIT        | https://github.com/yaml/pyyaml                      |

### Project support for CHF Quality Metric & MS-DRGs 

Please note the code in this project is provided for your exploration only, and are not formally supported by Databricks with Service Level Agreements (SLAs). They are provided AS-IS and we do not make any guarantees of any kind. Please do not submit a support ticket relating to any issues arising from the use of these projects. The source in this project is provided subject to the Databricks [License](./LICENSE). All included or referenced third party libraries are subject to the licenses set forth below.

Any issues discovered through the use of this project should be filed as GitHub Issues on the Repo. They will be reviewed as time permits, but there are no formal SLAs for support. 
