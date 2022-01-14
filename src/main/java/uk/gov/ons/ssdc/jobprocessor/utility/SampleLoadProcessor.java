package uk.gov.ons.ssdc.jobprocessor.utility;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.jobprocessor.transformer.NewCaseTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;

public class SampleLoadProcessor extends JobProcessor {
  private static final Transformer SAMPLE_LOAD_TRANSFORMER = new NewCaseTransformer();

  public SampleLoadProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.SAMPLE);
    setTransformer(SAMPLE_LOAD_TRANSFORMER);
    setColumnValidators(collectionExercise.getSurvey().getSampleValidationRules());
    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
    setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_SAMPLE);
    setFileViewProgressPermission(UserGroupAuthorisedActivityType.VIEW_SAMPLE_LOAD_PROGRESS);
  }
}
