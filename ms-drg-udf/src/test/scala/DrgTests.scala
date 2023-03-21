package com.databricks.labs.msdrg

import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.SparkSession

class DrgTests extends AnyFunSuite{
  test("Test drg grouper stand alone"){
    val input = DrgInput(0, "M", 0, "I120", "I120", 1, "E0800", "0TY00Z0,0FYG0Z0,5A1D70Z") //yields mdc=11, drg=019
    val output = Drg.process(input)
    assert(output.drgCode == "19")
    assert(output.grouperRC == "OK")
    assert(output.mdcCode == "11")
    assert(output.medSurgType == "SURGICAL")
    assert(output.mdcDescription == "Diseases and disorders of the kidney and urinary tract")
    assert(output.drgDescription == "Simultaneous pancreas and kidney transplant with hemodialysis")
  }

  test("Test drg grouper as Spark UDF"){
    val spark = SparkSession.builder()
      .master("local[2]")
      .config("spark.executor.instances", 1)
      .config("spark.driver.bindAddress","127.0.0.1") //Explicitly state this for Spark3.2.1+
      .getOrCreate()
    spark.sqlContext.udf.register("drgUDF", new com.databricks.labs.msdrg.DrgUDF().call _)

    val df = spark.sql(""" select drgUDF('0', 'M', '0', 'I120', 'I120', '1', 'E0800', '0TY00Z0,0FYG0Z0,5A1D70Z') as drg """)
    assert(df.count() === 1)
    assert(df.select("drg.drgCode").first.getString(0) === "19")
    assert(df.select("drg.grouperRC").first.getString(0) === "OK")
    assert(df.select("drg.mdcCode").first.getString(0) === "11")
    assert(df.select("drg.medSurgType").first.getString(0) === "SURGICAL")
    assert(df.select("drg.mdcDescription").first.getString(0) === "Diseases and disorders of the kidney and urinary tract")
    assert(df.select("drg.drgDescription").first.getString(0) === "Simultaneous pancreas and kidney transplant with hemodialysis")
    
  }
}

