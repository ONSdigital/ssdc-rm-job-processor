package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import lombok.Data;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.common.validation.MandatoryRule;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkInvalidCaseTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsInCollectionExerciseRule;

@Data
public class BulkInvalidTypeProcessor implements JobTypeProcessor {
  private static final Transformer BULK_INVALID_TRANSFORMER = new BulkInvalidCaseTransformer();
  private JobType jobType;
  private Transformer transformer;
  private ColumnValidator[] columnValidators;
  private String topic;

  public BulkInvalidTypeProcessor(String topic, String sharedPubsubProject) {
    setJobType(JobType.BULK_INVALID);
    setTransformer(BULK_INVALID_TRANSFORMER);
    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
  }

  @Override
  public ColumnValidator[] getColumnValidators(JobRow jobRow) {
    CollectionExercise collectionExercise = jobRow.getJob().getCollectionExercise();
    Rule[] caseExistsRules = {new CaseExistsInCollectionExerciseRule(collectionExercise)};
    ColumnValidator caseExistsValidator = new ColumnValidator("caseId", false, caseExistsRules);

    Rule[] reasonRule = {new MandatoryRule()};
    ColumnValidator reasonRuleValidator = new ColumnValidator("reason", false, reasonRule);

    return new ColumnValidator[] {caseExistsValidator, reasonRuleValidator};
  }
}
