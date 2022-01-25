package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.common.validation.MandatoryRule;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkInvalidCaseTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsInCollectionExerciseRule;

public class BulkInvalidTypeProcessor extends JobTypeProcessor {
  private static final Transformer BULK_INVALID_TRANSFORMER = new BulkInvalidCaseTransformer();

  public BulkInvalidTypeProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.BULK_INVALID);
    setTransformer(BULK_INVALID_TRANSFORMER);
    setColumnValidators(getBulkInvalidCaseValidationRules(collectionExercise));
    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
    setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_BULK_INVALID);
    setFileViewProgressPermission(UserGroupAuthorisedActivityType.VIEW_BULK_INVALID_PROGRESS);
  }

  private ColumnValidator[] getBulkInvalidCaseValidationRules(
      CollectionExercise collectionExercise) {
    Rule[] caseExistsRules = {new CaseExistsInCollectionExerciseRule(collectionExercise)};
    ColumnValidator caseExistsValidator = new ColumnValidator("caseId", false, caseExistsRules);

    Rule[] reasonRule = {new MandatoryRule()};
    ColumnValidator reasonRuleValidator = new ColumnValidator("reason", false, reasonRule);

    return new ColumnValidator[] {caseExistsValidator, reasonRuleValidator};
  }
}
