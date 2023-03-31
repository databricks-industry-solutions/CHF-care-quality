# Congestive Heart Failure Preventative Quality Metrics

## Assumptions

1. Input Data: We are assuming the 5 datasets from [CMS Synthetic Public Use Files](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF) are available. Data dictionaries are available publicly with [Resdac ffs](https://resdac.org/cms-data?tid_1%5B1%5D=1&tid%5B6046%5D=6046&tid%5B4931%5D=4931)

2. CMS SynPuf is mimics EDI claim transactions in a healthcare setting. The datatypes and meaning of fields used should be very similar to existing claims datasets. 

## Input Data

Database: **hls_cms_synpuf**

| Tablenames | Description | 
| -- | -- | 
| **BEN_SUM** | Member benefits information | 
| **CAR_CLAIMS** | Carrier claims, aka mostly PCP & outpatient related claim data |
| **INP_CLAIMS** | Inpatient claims data |
| **OUT_CLAIMS** | Outpatient claims data |
| **RX_CLAIMS** | Pharmacy claims (non-medical services) |

## Running the notebook

```
"Run All" inside the databricks notebook will
```
1. Calculate denominator values
2. Calculate numerator values
3. Aggregate scores for provider quality (by ACO by Year, etc)


### For claims prior to 10-01-2015 or CMS SynPuf

We follow closely to this [document](https://www.cms.gov/files/document/aco-10-prevention-quality-indicator-pqi-ambulatory-sensitive-conditions-admissions-heart-failure-hf.pdf). Note that our dataset is pre 2015 so our dataset has ICD9 codes and so we follow an ICD9 spec

### For claims post 10-01-2015

[This](https://qualityindicators.ahrq.gov/Downloads/Modules/PQI/V2022/TechSpecs/PQI_08_Heart_Failure_Admission_Rate.pdf) is an updated version from AHRQ with the ICD10 codes listed 

## Working with Output 

A sample look at the worst performing ACOs for preventative care around Congestive Heart Failure 

![alt text](../images/aco_results.png?raw=true)


