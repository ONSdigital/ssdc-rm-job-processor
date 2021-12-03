package uk.gov.ons.ssdc.jobprocessor.transformer;

import java.util.Map;
import java.util.UUID;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.EventDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.EventHeaderDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.InvalidCaseDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.PayloadDTO;
import uk.gov.ons.ssdc.jobprocessor.utility.EventHelper;

public class BulkInvalidCaseTransformer implements Transformer {

  @Override
  public Object transformRow(
      Job job, JobRow jobRow, ColumnValidator[] columnValidators, String topic) {
    Map<String, String> rowData = jobRow.getRowData();

    InvalidCaseDTO invalidCaseDTO = new InvalidCaseDTO();
    invalidCaseDTO.setCaseId(UUID.fromString(rowData.get("caseId")));
    invalidCaseDTO.setReason(rowData.get("reason"));

    PayloadDTO payloadDTO = new PayloadDTO();
    payloadDTO.setInvalidCase(invalidCaseDTO);

    EventDTO event = new EventDTO();
    EventHeaderDTO eventHeader = EventHelper.createEventDTO(topic, job.getProcessedBy());
    eventHeader.setCorrelationId(job.getId());
    event.setHeader(eventHeader);
    event.setPayload(payloadDTO);

    return event;
  }
}
