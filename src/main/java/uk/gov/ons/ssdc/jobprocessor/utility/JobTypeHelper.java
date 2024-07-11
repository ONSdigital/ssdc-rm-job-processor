package uk.gov.ons.ssdc.jobprocessor.utility;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.BulkInvalidTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.BulkRefusalTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.BulkUpdateSampleTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.BulkUpdateSensitiveSampleTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.JobTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.SampleLoadTypeProcessor;

@Component
public class JobTypeHelper {
  //  We load these topics etc in here rather than in the POJOS
  //  as this has nice @value wiring as a Component
  @Value("${spring.cloud.gcp.pubsub.project-id}")
  private String pubsubProject;

  @Value("${queueconfig.new-case-topic}")
  private String newCaseTopic;

  @Value("${queueconfig.refusal-event-topic}")
  private String refusalEventTopic;

  @Value("${queueconfig.invalid-case-event-topic}")
  private String invalidCaseTopic;

  @Value("${queueconfig.update-sample-topic}")
  private String updateSampleTopic;

  @Value("${queueconfig.update-sample-sensitive-topic}")
  private String updateSensitiveSampleTopic;

  public JobTypeProcessor getJobTypeProcessor(
      JobType jobType, CollectionExercise collectionExercise) {

    if (collectionExercise == null) {
      throw new RuntimeException("CollectionExercise is null!");
    }

    switch (jobType) {
      case SAMPLE:
        return new SampleLoadTypeProcessor(newCaseTopic, pubsubProject, collectionExercise);

      case BULK_REFUSAL:
        return new BulkRefusalTypeProcessor(refusalEventTopic, pubsubProject, collectionExercise);

      case BULK_INVALID:
        return new BulkInvalidTypeProcessor(invalidCaseTopic, pubsubProject, collectionExercise);

      case BULK_UPDATE_SAMPLE:
        return new BulkUpdateSampleTypeProcessor(
            updateSampleTopic, pubsubProject, collectionExercise);

      case BULK_UPDATE_SAMPLE_SENSITIVE:
        return new BulkUpdateSensitiveSampleTypeProcessor(
            updateSensitiveSampleTopic, pubsubProject, collectionExercise);

      default:
        // This code should be unreachable, providing we have a case for every JobType
        throw new RuntimeException(
            String.format("In getJobTypeSettings the jobType %s wasn't matched", jobType));
    }
  }
}
