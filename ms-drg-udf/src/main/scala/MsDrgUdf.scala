package com.databricks.labs.msdrg

import org.apache.spark.sql.api.java.UDF8

/*
 * UDF to run MS-DRG Grouper
 * @input fields needed for DRG
 * @output a map of key/value pairs with relevant drg results, mdc, drg, descriptions
 */

class DrgUDF extends UDF8[String, String, String, String, String, String, String, String, DrgOutput]{

  override def call(age: String, sex: String, lengthOfStay: String, admitDiagnosis: String, principalDiagnosis: String, dischargeStatus: String, diagnosis: String, procedures: String): DrgOutput = {

    //Parse DRG Input
    var in = None: Option[DrgInput]
    try{
      in = Some(DrgInput(age.toInt,
        sex,
        lengthOfStay.toInt,
        admitDiagnosis,
        principalDiagnosis,
        { if (dischargeStatus.toUpperCase == "U") -1 else dischargeStatus.toInt },
        diagnosis,
        procedures
      ))
    }catch{
      case e: Throwable =>
        return DrgOutput("DRG Input Error: " + e.toString)
    }

    //Execute DRG
    var out = None: Option[DrgOutput]
    try{
      out = Some(Drg.process(in.get))
    }catch{
      case e: Throwable =>
        return DrgOutput("DRG Execute Error: " + e.toString)
    }
    out match {
      case Some(x) => return x
      case _ => return DrgOutput("Drg Execute Error: Did not get a valid output from DRG Grouper")
    }
  }
}


object DrgUDF
