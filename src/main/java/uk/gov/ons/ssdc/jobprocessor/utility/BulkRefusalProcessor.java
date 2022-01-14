package uk.gov.ons.ssdc.jobprocessor.utility;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkRefusalTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;

public class BulkRefusalProcessor extends JobProcessor {
  private static final Transformer BULK_REFUSAL_TRANSFORMER = new BulkRefusalTransformer();

  public BulkRefusalProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.BULK_REFUSAL);
    setTransformer(BULK_REFUSAL_TRANSFORMER);
    setColumnValidators(collectionExercise.getSurvey().getSampleValidationRules());
    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
    setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_BULK_REFUSAL);
    setFileViewProgressPermission(UserGroupAuthorisedActivityType.VIEW_BULK_REFUSAL_PROGRESS);
  }
}
