package uk.gov.ons.ssdc.jobprocessor.transformer;

import java.util.Map;
import java.util.UUID;
import org.springframework.context.ApplicationContext;
import uk.gov.ons.ssdc.common.model.entity.Case;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.config.ApplicationContextProvider;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.EventDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.EventHeaderDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.PayloadDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.UpdateSample;
import uk.gov.ons.ssdc.jobprocessor.repository.CaseRepository;
import uk.gov.ons.ssdc.jobprocessor.utility.EventHelper;

public class BulkPHMUpdateSampleTransformer implements Transformer {
  private static CaseRepository caseRepository = null;

  @Override
  public Object transformRow(
      Job job, JobRow jobRow, ColumnValidator[] columnValidators, String topic) {
    Map<String, String> rowData = jobRow.getRowData();

    Case caze =
        getCaseRepository()
            .findCaseByParticipantIdAndCollectionExercise(
                rowData.get("PARTICIPANT_ID"), job.getCollectionExercise().getId());

    UpdateSample updateSample = new UpdateSample();
    updateSample.setCaseId(UUID.fromString(caze.getId().toString()));

    updateSample.setSample(rowData);

    PayloadDTO payloadDTO = new PayloadDTO();
    payloadDTO.setUpdateSample(updateSample);

    EventDTO event = new EventDTO();
    EventHeaderDTO eventHeader = EventHelper.createEventDTO(topic, job.getProcessedBy());
    eventHeader.setCorrelationId(job.getId());
    event.setHeader(eventHeader);
    event.setPayload(payloadDTO);

    return event;
  }

  private CaseRepository getCaseRepository() {
    if (caseRepository == null) {
      ApplicationContext applicationContext = ApplicationContextProvider.getApplicationContext();
      caseRepository = applicationContext.getBean(CaseRepository.class);
    }

    return caseRepository;
  }
}
