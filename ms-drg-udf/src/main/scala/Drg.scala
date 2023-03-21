package com.databricks.labs.msdrg

import scala.collection.JavaConverters._
import com.mmm.his.cer.foundation.exception.FoundationException
import com.mmm.his.cer.foundation.model.GfcPoa
import gov.agency.msdrg.model.MsdrgOption
import gov.agency.msdrg.model.MsdrgRuntimeOption
import gov.agency.msdrg.model.RuntimeOptions
import gov.agency.msdrg.model.enumeration.MarkingLogicTieBreaker
import gov.agency.msdrg.model.enumeration.MsdrgAffectDrgOptionFlag
import gov.agency.msdrg.model.enumeration.MsdrgDischargeStatus
import gov.agency.msdrg.model.enumeration.MsdrgHospitalStatusOptionFlag
import gov.agency.msdrg.model.enumeration.MsdrgSex
import gov.agency.msdrg.model.transfer.MsdrgClaim
import gov.agency.msdrg.model.transfer.MsdrgInput
import gov.agency.msdrg.model.transfer.MsdrgInputDxCode
import gov.agency.msdrg.model.transfer.MsdrgInputPrCode
import gov.agency.msdrg.model.transfer.MsdrgOutputData
import gov.agency.msdrg.v391.MsdrgComponent
import java.util.ArrayList
import java.util.List
import java.util.Optional

object Drg{

  val drgBuilder = MsdrgInput.builder()
  val options = new RuntimeOptions
  options.setPoaReportingExempt(MsdrgHospitalStatusOptionFlag.NON_EXEMPT)
  options.setComputeAffectDrg(MsdrgAffectDrgOptionFlag.COMPUTE)
  options.setMarkingLogicTieBreaker(MarkingLogicTieBreaker.CLINICAL_SIGNIFICANCE)

  val runtimeOptions = new MsdrgRuntimeOption
  runtimeOptions.put(MsdrgOption.RUNTIME_OPTION_FLAGS, options)

  val component = new MsdrgComponent(runtimeOptions)

  //  def process(input: DrgInput): DrgOutput = {

  def process(input: DrgInput): DrgOutput = {
    //set input values
    val drgInput = drgBuilder
      .withAdmissionDiagnosisCode(new MsdrgInputDxCode(input.admitDiagnosis, GfcPoa.Y))
      .withPrincipalDiagnosisCode(new MsdrgInputDxCode(input.principalDiagnosis, GfcPoa.Y))
      .withSecondaryDiagnosisCodes(input.diagnosis.split(",").map(new MsdrgInputDxCode(_, GfcPoa.Y)).toList.asJava)
      .withAgeDaysAdmit(0)
      .withAgeDaysDischarge(input.lengthOfStay)
      .withAgeInYears(input.age)
      .withDischargeStatus(MsdrgDischargeStatus.values.find(_.intValue == input.dischargeStatus).getOrElse(MsdrgDischargeStatus.NONE) )
      .withProcedureCodes( input.procedures.split(",").map(new MsdrgInputPrCode(_)).toList.asJava )
      .withSex({if (input.sex.toUpperCase.startsWith("M")) MsdrgSex.MALE else MsdrgSex.FEMALE } )
      .build

    val claim = new MsdrgClaim(drgInput)
    component.process(claim)
    val output = claim.getOutput.get

    return DrgOutput(
      output.getFinalGrc.toString,
      output.getFinalDrg.getValue.toString,
      output.getFinalDrg.getDescription,
      output.getFinalMdc.getValue.toString,
      output.getFinalMdc.getDescription,
      output.getFinalMedSugType.toString
    )
  }
} 

