package uk.gov.ons.ssdc.jobprocessor.utility;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobType;

@Component
public class JobTypeHelper {
  //  We load these topics etc in here rather than in the POJOS
  //  as this has nice @value wiring as a Component
  @Value("${queueconfig.shared-pubsub-project}")
  private String sharedPubsubProject;

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

  public JobProcessor getJobTypeProcessor(JobType jobType, CollectionExercise collectionExercise) {

    if (collectionExercise == null) {
      throw new RuntimeException("CollectionExercise is null!");
    }

    switch (jobType) {
      case SAMPLE:
        return new SampleLoadProcessor(newCaseTopic, sharedPubsubProject, collectionExercise);

      case BULK_REFUSAL:
        return new BulkRefusalProcessor(refusalEventTopic, sharedPubsubProject, collectionExercise);

      case BULK_INVALID:
        return new BulkInvalidProcessor(invalidCaseTopic, sharedPubsubProject, collectionExercise);

      case BULK_UPDATE_SAMPLE:
        return new BulkUpdateSampleProcessor(
            updateSampleTopic, sharedPubsubProject, collectionExercise);

      case BULK_UPDATE_SAMPLE_SENSITIVE:
        return new BulkUpdateSensitiveSampleProcessor(
            updateSensitiveSampleTopic, sharedPubsubProject, collectionExercise);

      default:
        // This code should be unreachable, providing we have a case for every JobType
        throw new RuntimeException(
            String.format("In getJobTypeSettings the jobType %s wasn't matched", jobType));
    }
  }
}
