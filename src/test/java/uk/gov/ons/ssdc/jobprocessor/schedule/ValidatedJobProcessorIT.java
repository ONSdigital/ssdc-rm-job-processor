package uk.gov.ons.ssdc.jobprocessor.schedule;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.gov.ons.ssdc.common.model.entity.Case;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.common.model.entity.JobStatus;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.EventDTO;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.RefusalTypeDTO;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;
import uk.gov.ons.ssdc.jobprocessor.testutils.JunkDataHelper;
import uk.gov.ons.ssdc.jobprocessor.testutils.PubsubHelper;
import uk.gov.ons.ssdc.jobprocessor.testutils.QueueSpy;

@ContextConfiguration
@ActiveProfiles("test")
@SpringBootTest
@ExtendWith(SpringExtension.class)
public class ValidatedJobProcessorIT {
  private static final String NEW_CASE_SUBSCRIPTION = "event_new-case_rm-case-processor";
  private static final String REFUSAL_SUBSCRIPTION = "event_refusal_rm-case-processor";

  @Autowired private JobRepository jobRepository;

  @Autowired private JobRowRepository jobRowRepository;

  @Autowired private JunkDataHelper junkDataHelper;

  @Autowired private PubsubHelper pubsubHelper;

  @Value("${queueconfig.new-case-topic}")
  private String newCaseTopic;

  @Value("${queueconfig.refusal-event-topic}")
  private String refusalEventTopic;

  @BeforeEach
  public void setUp() {
    pubsubHelper.purgeSharedProjectMessages(NEW_CASE_SUBSCRIPTION, newCaseTopic);
    pubsubHelper.purgeSharedProjectMessages(REFUSAL_SUBSCRIPTION, refusalEventTopic);
  }

  @Test
  void processStagedJobsSample() throws InterruptedException {
    try (QueueSpy<EventDTO> surveyUpdateQueue =
        pubsubHelper.sharedProjectListen(NEW_CASE_SUBSCRIPTION, EventDTO.class)) {
      CollectionExercise collectionExercise = junkDataHelper.setupJunkCollex();

      Job job = new Job();
      job.setId(UUID.randomUUID());
      job.setCollectionExercise(collectionExercise);
      job.setJobStatus(JobStatus.VALIDATED_OK);
      job.setJobType(JobType.SAMPLE);
      job.setCreatedBy("norman");
      job.setCreatedAt(OffsetDateTime.now());
      job.setFileId(UUID.randomUUID());
      job.setFileName("normansfile.csv");
      job = jobRepository.saveAndFlush(job);

      JobRow jobRow = new JobRow();
      jobRow.setId(UUID.randomUUID());
      jobRow.setJob(job);
      jobRow.setJobRowStatus(JobRowStatus.VALIDATED_OK);
      jobRow.setRowData(Map.of("Junk", "test junk", "SensitiveJunk", "sensitive"));
      jobRow.setOriginalRowData(new String[] {"foo", "bar"});
      jobRowRepository.saveAndFlush(jobRow);

      // This will unleash the hounds
      job.setJobStatus(JobStatus.PROCESSING_IN_PROGRESS);
      jobRepository.saveAndFlush(job);

      // Now check that the job processed OK
      EventDTO emittedEvent = surveyUpdateQueue.getQueue().poll(20, TimeUnit.SECONDS);
      assertThat(emittedEvent).isNotNull();
      assertThat(emittedEvent.getPayload().getNewCase()).isNotNull();
      assertThat(emittedEvent.getPayload().getNewCase().getCollectionExerciseId())
          .isEqualTo(collectionExercise.getId());
      assertThat(emittedEvent.getPayload().getNewCase().getSample().get("Junk"))
          .isEqualTo("test junk");
      assertThat(emittedEvent.getPayload().getNewCase().getSampleSensitive().get("SensitiveJunk"))
          .isEqualTo("sensitive");

      Job processedJob = jobRepository.findById(job.getId()).get();
      assertThat(processedJob.getJobStatus()).isEqualTo(JobStatus.PROCESSED);
      assertThat(processedJob.getProcessingRowNumber()).isEqualTo(1);

      Optional<JobRow> optionalJobRow = jobRowRepository.findById(jobRow.getId());
      assertThat(optionalJobRow.isPresent()).isFalse();
    }
  }

  @Test
  void processStagedJobsBulkRefusal() throws InterruptedException {
    try (QueueSpy<EventDTO> surveyUpdateQueue =
        pubsubHelper.sharedProjectListen(REFUSAL_SUBSCRIPTION, EventDTO.class)) {
      Case caze = junkDataHelper.setupJunkCase();
      CollectionExercise collectionExercise = caze.getCollectionExercise();

      Job job = new Job();
      job.setId(UUID.randomUUID());
      job.setCollectionExercise(collectionExercise);
      job.setJobStatus(JobStatus.VALIDATED_OK);
      job.setJobType(JobType.BULK_REFUSAL);
      job.setCreatedBy("norman");
      job.setCreatedAt(OffsetDateTime.now());
      job.setFileId(UUID.randomUUID());
      job.setFileName("normansfile.csv");
      job = jobRepository.saveAndFlush(job);

      JobRow jobRow = new JobRow();
      jobRow.setId(UUID.randomUUID());
      jobRow.setJob(job);
      jobRow.setJobRowStatus(JobRowStatus.VALIDATED_OK);
      jobRow.setRowData(Map.of("caseId", caze.getId().toString(), "refusalType", "HARD_REFUSAL"));
      jobRow.setOriginalRowData(new String[] {"foo", "bar"});
      jobRowRepository.saveAndFlush(jobRow);

      // This will unleash the hounds
      job.setJobStatus(JobStatus.PROCESSING_IN_PROGRESS);
      jobRepository.saveAndFlush(job);

      // Now check that the job processed OK
      EventDTO emittedEvent = surveyUpdateQueue.getQueue().poll(20, TimeUnit.SECONDS);
      assertThat(emittedEvent).isNotNull();
      assertThat(emittedEvent.getPayload().getRefusal()).isNotNull();
      assertThat(emittedEvent.getPayload().getRefusal().getCaseId()).isEqualTo(caze.getId());
      assertThat(emittedEvent.getPayload().getRefusal().getType())
          .isEqualTo(RefusalTypeDTO.HARD_REFUSAL);

      Job processedJob = jobRepository.findById(job.getId()).get();
      assertThat(processedJob.getJobStatus()).isEqualTo(JobStatus.PROCESSED);
      assertThat(processedJob.getProcessingRowNumber()).isEqualTo(1);

      Optional<JobRow> optionalJobRow = jobRowRepository.findById(jobRow.getId());
      assertThat(optionalJobRow.isPresent()).isFalse();
    }
  }
}
