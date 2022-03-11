package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import java.util.Map;
import lombok.Data;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkUpdateSampleTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;

@Data
public class BulkUpdateSampleTypeProcessor implements JobTypeProcessor {
  private static final Transformer BULK_SAMPLE_UPDATE_TRANSFORMER =
      new BulkUpdateSampleTransformer();
  private JobType jobType;
  private Transformer transformer;
  private ColumnValidator[] columnValidators;
  private String topic;
  private Map<String, ColumnValidator[]> sampleDataColumnMaps;

  public BulkUpdateSampleTypeProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.BULK_UPDATE_SAMPLE);
    setTransformer(BULK_SAMPLE_UPDATE_TRANSFORMER);

    setColumnValidators(SampleAndSensitiveSampleHelper.getBulkSampleValidationRulesHeaderRowOnly());

    sampleDataColumnMaps =
        SampleAndSensitiveSampleHelper.getSampleAndSensitiveDataColumnMaps(
            collectionExercise.getSurvey().getSampleValidationRules(),
            collectionExercise,
            JobType.BULK_UPDATE_SAMPLE);

    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
  }

  @Override
  public ColumnValidator[] getColumnValidators(JobRow jobRow)
      throws ValidatorFieldNotFoundException {
    String fieldToUpdate = jobRow.getRowData().get("fieldToUpdate");
    return SampleAndSensitiveSampleHelper.getColumnValidatorForSampleOrSensitiveDataRows(
        sampleDataColumnMaps, fieldToUpdate);
  }
}
