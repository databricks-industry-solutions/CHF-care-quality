package com.databricks.labs.msdrg


case class DrgInput
(
  age: Integer,
  sex: String = "",
  lengthOfStay: Integer = 0,
  admitDiagnosis: String, //Tyipcally first dx
  principalDiagnosis: String, //Primary dx
  dischargeStatus: Integer = null, //This is the UB04 discharge code. Default to "unknown/blank"
  diagnosis: String = "", //Expecting a comma seperated list of diagnosis codes
  procedures: String = "", //Expecting a comma seperated list of proc codes
)
