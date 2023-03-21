package com.databricks.labs.msdrg

case class DrgOutput
(
  grouperRC: String,
  drgCode: String="999",
  drgDescription: String="Ungroupable",
  mdcCode: String= "-1",
  mdcDescription: String="NA",
  medSurgType: String="NA", //medical vs surgical indicator of the DRG
){

  def toMap(): Map[String,String] = {
    return Map("grouperRC" -> grouperRC,
      "drgCode" -> drgCode,
      "drgDescription" -> drgDescription,
      "mdcCode" -> mdcCode,
      "mdcDescription" -> mdcDescription,
      "medSurgType" -> medSurgType
    )
  }
}
