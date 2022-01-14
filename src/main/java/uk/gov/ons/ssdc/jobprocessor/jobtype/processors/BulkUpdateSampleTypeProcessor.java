package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkUpdateSampleTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;

public class BulkUpdateSampleTypeProcessor extends JobTypeProcessor {
  private static final Transformer BULK_SAMPLE_UPDATE_TRANSFORMER =
      new BulkUpdateSampleTransformer();

  public BulkUpdateSampleTypeProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.BULK_UPDATE_SAMPLE);
    setTransformer(BULK_SAMPLE_UPDATE_TRANSFORMER);

    setColumnValidators(getBulkSampleValidationRulesHeaderRowOnly());
    setSampleAndSensitiveDataColumnMaps(
        collectionExercise.getSurvey().getSampleValidationRules(), collectionExercise);
    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
    setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_BULK_UPDATE_SAMPLE);
    setFileViewProgressPermission(UserGroupAuthorisedActivityType.VIEW_BULK_UPDATE_SAMPLE_PROGRESS);
  }

  public ColumnValidator[] getColumnValidators(JobRow jobRow)
      throws ValidatorFieldNotFoundException {
    String fieldToUpdate = jobRow.getRowData().get("fieldToUpdate");
    return getColumnValidatorForSampleOrSensitiveDataRows(fieldToUpdate);
  }
}
