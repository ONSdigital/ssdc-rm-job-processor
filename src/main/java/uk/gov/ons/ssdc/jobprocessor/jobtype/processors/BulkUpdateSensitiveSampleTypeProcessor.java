package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkUpdateSensitiveTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;

public class BulkUpdateSensitiveSampleTypeProcessor extends JobTypeProcessor {

  private static final Transformer BULK_SENSITIVE_UPDATE_TRANSFORMER =
      new BulkUpdateSensitiveTransformer();

  public BulkUpdateSensitiveSampleTypeProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.BULK_UPDATE_SAMPLE_SENSITIVE);
    setTransformer(BULK_SENSITIVE_UPDATE_TRANSFORMER);

    setColumnValidators(getBulkSampleValidationRulesHeaderRowOnly());
    setSampleAndSensitiveDataColumnMaps(
        collectionExercise.getSurvey().getSampleValidationRules(), collectionExercise);
    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
    setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_BULK_UPDATE_SAMPLE_SENSITIVE);
    setFileViewProgressPermission(
        UserGroupAuthorisedActivityType.VIEW_BULK_UPDATE_SAMPLE_SENSITIVE_PROGRESS);
  }

  @Override
  public ColumnValidator[] getColumnValidators(JobRow jobRow)
      throws ValidatorFieldNotFoundException {

    if ("".equals(jobRow.getRowData().get("newValue"))) {
      // We should disregard all validation rules if we're blanking out the data
      return new ColumnValidator[0];
    }

    String fieldToUpdate = jobRow.getRowData().get("fieldToUpdate");
    return getColumnValidatorForSampleOrSensitiveDataRows(fieldToUpdate);
  }
}
