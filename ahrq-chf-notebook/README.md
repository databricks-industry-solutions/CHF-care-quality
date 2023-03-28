# Congestive Heart Failure Preventative Quality Metrics

## Assumptions

1. Input Data: We are assuming the 5 datasets from [CMS Synthetic Public Use Files](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs/DE_Syn_PUF) are available in a database. 

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

Calc Denominator inclusion/exclusion
Calc Numerator (subset of denom) inclusion/exclusion
Aggregate within provider group (ACO, Hospital, etc)

### For claims prior to 10-01-2015 or CMS SynPuf


### For claims post 10-01-2015


## Working with Output 

Sample records... prvdr + numerator + denominator + score


