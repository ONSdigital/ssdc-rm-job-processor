package uk.gov.ons.ssdc.jobprocessor.transformer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.EventDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.EventHeaderDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.PayloadDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.UpdateSampleSensitive;
import uk.gov.ons.ssdc.jobprocessor.utility.EventHelper;

public class BulkUpdateSensitiveTransformer implements Transformer {

  @Override
  public Object transformRow(
      Job job, JobRow jobRow, ColumnValidator[] columnValidators, String topic) {
    Map<String, String> rowData = jobRow.getRowData();

    UpdateSampleSensitive updateSampleSensitive = new UpdateSampleSensitive();
    updateSampleSensitive.setCaseId(UUID.fromString(rowData.get("caseId")));

    Map<String, String> sensitiveUpdateMap = new HashMap<>();
    sensitiveUpdateMap.put(
        jobRow.getRowData().get("fieldToUpdate"), jobRow.getRowData().get("newValue"));
    updateSampleSensitive.setSampleSensitive(sensitiveUpdateMap);

    PayloadDTO payloadDTO = new PayloadDTO();
    payloadDTO.setUpdateSampleSensitive(updateSampleSensitive);

    EventDTO event = new EventDTO();
    EventHeaderDTO eventHeader = EventHelper.createEventDTO(topic, job.getProcessedBy());
    eventHeader.setCorrelationId(job.getId());
    event.setHeader(eventHeader);
    event.setPayload(payloadDTO);

    return event;
  }
}
