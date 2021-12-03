package uk.gov.ons.ssdc.jobprocessor.model.dto.messaging;

import java.util.Map;
import java.util.UUID;
import lombok.Data;

@Data
public class UpdateSampleSensitive {
  private UUID caseId;
  private Map<String, String> sampleSensitive;
}
