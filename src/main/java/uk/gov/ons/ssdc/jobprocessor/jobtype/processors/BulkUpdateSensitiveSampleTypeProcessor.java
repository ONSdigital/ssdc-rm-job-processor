package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import java.util.Map;
import lombok.Data;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkUpdateSensitiveTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;

@Data
public class BulkUpdateSensitiveSampleTypeProcessor implements JobTypeProcessor {

  private static final Transformer BULK_SENSITIVE_UPDATE_TRANSFORMER =
      new BulkUpdateSensitiveTransformer();
  private JobType jobType;
  private Transformer transformer;
  private ColumnValidator[] columnValidators;
  private String topic;
  private Map<String, ColumnValidator[]> sensitiveDataColumnMaps;

  public BulkUpdateSensitiveSampleTypeProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    jobType = JobType.BULK_UPDATE_SAMPLE_SENSITIVE;
    transformer = BULK_SENSITIVE_UPDATE_TRANSFORMER;

    sensitiveDataColumnMaps =
        SampleAndSensitiveSampleHelper.getSampleAndSensitiveDataColumnMaps(
            collectionExercise.getSurvey().getSampleValidationRules(),
            collectionExercise,
            JobType.BULK_UPDATE_SAMPLE_SENSITIVE);

    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
  }

  @Override
  public ColumnValidator[] getColumnValidators(JobRow jobRow)
      throws ValidatorFieldNotFoundException {

    if ("".equals(jobRow.getRowData().get("newValue"))) {
      // We should disregard all validation rules if we're blanking out the data
      return new ColumnValidator[0];
    }

    String fieldToUpdate = jobRow.getRowData().get("fieldToUpdate");
    return SampleAndSensitiveSampleHelper.getColumnValidatorForSampleOrSensitiveDataRows(
        sensitiveDataColumnMaps, fieldToUpdate);
  }
}
