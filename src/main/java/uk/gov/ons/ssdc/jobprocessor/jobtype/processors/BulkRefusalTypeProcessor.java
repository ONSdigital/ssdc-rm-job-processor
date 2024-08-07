package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import java.util.EnumSet;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.common.validation.InSetRule;
import uk.gov.ons.ssdc.common.validation.MandatoryRule;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.RefusalTypeDTO;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkRefusalTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsInCollectionExerciseRule;

public class BulkRefusalTypeProcessor extends JobTypeProcessor {
  private static final Transformer BULK_REFUSAL_TRANSFORMER = new BulkRefusalTransformer();

  public BulkRefusalTypeProcessor(
      String topic, String pubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.BULK_REFUSAL);
    setTransformer(BULK_REFUSAL_TRANSFORMER);
    setColumnValidators(getBulkRefusalProcessorValidationRules(collectionExercise));
    setTopic(toProjectTopicName(topic, pubsubProject).toString());
    setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_BULK_REFUSAL);
    setFileViewProgressPermission(UserGroupAuthorisedActivityType.VIEW_BULK_REFUSAL_PROGRESS);
  }

  private ColumnValidator[] getBulkRefusalProcessorValidationRules(
      CollectionExercise collectionExercise) {
    Rule[] caseExistsRules = {new CaseExistsInCollectionExerciseRule(collectionExercise)};
    ColumnValidator caseExistsValidator = new ColumnValidator("caseId", false, caseExistsRules);

    String[] refusalTypes =
        EnumSet.allOf(RefusalTypeDTO.class).stream().map(Enum::toString).toArray(String[]::new);
    Rule[] refusalSetRules = {new InSetRule(refusalTypes), new MandatoryRule()};

    ColumnValidator refusalTypeValidator =
        new ColumnValidator("refusalType", false, refusalSetRules);

    return new ColumnValidator[] {caseExistsValidator, refusalTypeValidator};
  }

  @Override
  public ColumnValidator[] getColumnValidators(JobRow jobRow)
      throws ValidatorFieldNotFoundException {
    return columnValidators.clone();
  }
}
