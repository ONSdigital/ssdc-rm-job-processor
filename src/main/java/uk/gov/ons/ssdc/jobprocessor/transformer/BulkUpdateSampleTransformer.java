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
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.UpdateSample;
import uk.gov.ons.ssdc.jobprocessor.utility.EventHelper;

public class BulkUpdateSampleTransformer implements Transformer {

  @Override
  public Object transformRow(
      Job job, JobRow jobRow, ColumnValidator[] columnValidators, String topic) {
    Map<String, String> rowData = jobRow.getRowData();

    UpdateSample updateSample = new UpdateSample();
    updateSample.setCaseId(UUID.fromString(rowData.get("caseId")));

    Map<String, String> sampleUpdateMap = new HashMap<>();
    sampleUpdateMap.put(
        jobRow.getRowData().get("fieldToUpdate"), jobRow.getRowData().get("newValue"));
    updateSample.setSample(sampleUpdateMap);

    PayloadDTO payloadDTO = new PayloadDTO();
    payloadDTO.setUpdateSample(updateSample);

    EventDTO event = new EventDTO();
    EventHeaderDTO eventHeader = EventHelper.createEventDTO(topic, job.getProcessedBy());
    eventHeader.setCorrelationId(job.getId());
    event.setHeader(eventHeader);
    event.setPayload(payloadDTO);

    return event;
  }
}
