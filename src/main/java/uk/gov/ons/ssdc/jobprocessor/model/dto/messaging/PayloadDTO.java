package uk.gov.ons.ssdc.jobprocessor.model.dto.messaging;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import lombok.Data;

@Data
@JsonInclude(Include.NON_NULL)
public class PayloadDTO {
  private RefusalDTO refusal;
  private InvalidCaseDTO invalidCase;
  private UpdateSample updateSample;
  private UpdateSampleSensitive updateSampleSensitive;
  private NewCase newCase;
}
