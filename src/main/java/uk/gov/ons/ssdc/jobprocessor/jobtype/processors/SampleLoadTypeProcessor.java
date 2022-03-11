package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import java.util.Map;
import lombok.Data;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.transformer.NewCaseTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;

@Data
public class SampleLoadTypeProcessor implements JobTypeProcessor {
  private static final Transformer SAMPLE_LOAD_TRANSFORMER = new NewCaseTransformer();
  private JobType jobType;
  private Transformer transformer;
  private ColumnValidator[] columnValidators;
  private String topic;
  private Map<String, ColumnValidator[]> sensitiveDataColumnMaps;

  public SampleLoadTypeProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.SAMPLE);
    setTransformer(SAMPLE_LOAD_TRANSFORMER);
    setColumnValidators(collectionExercise.getSurvey().getSampleValidationRules());
    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
  }

  @Override
  public ColumnValidator[] getColumnValidators(JobRow jobRow)
      throws ValidatorFieldNotFoundException {
    return columnValidators.clone();
  }
}
