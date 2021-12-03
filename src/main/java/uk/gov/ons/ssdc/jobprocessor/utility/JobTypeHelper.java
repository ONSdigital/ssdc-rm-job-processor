package uk.gov.ons.ssdc.jobprocessor.utility;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import java.util.EnumSet;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.common.validation.InSetRule;
import uk.gov.ons.ssdc.common.validation.MandatoryRule;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.RefusalTypeDTO;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkInvalidCaseTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkRefusalTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkUpdateSampleTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkUpdateSensitiveTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.NewCaseTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsInCollectionExerciseRule;

@Component
public class JobTypeHelper {
  private static final Transformer SAMPLE_LOAD_TRANSFORMER = new NewCaseTransformer();
  private static final Transformer BULK_REFUSAL_TRANSFORMER = new BulkRefusalTransformer();
  private static final Transformer BULK_INVALID_TRANSFORMER = new BulkInvalidCaseTransformer();
  private static final Transformer BULK_SAMPLE_UPDATE_TRANSFORMER =
      new BulkUpdateSampleTransformer();
  private static final Transformer BULK_SENSITIVE_UPDATE_TRANSFORMER =
      new BulkUpdateSensitiveTransformer();

  @Value("${queueconfig.shared-pubsub-project}")
  private String sharedPubsubProject;

  @Value("${queueconfig.new-case-topic}")
  private String newCaseTopic;

  @Value("${queueconfig.refusal-event-topic}")
  private String refusalEventTopic;

  @Value("${queueconfig.invalid-case-event-topic}")
  private String invalidCaseTopic;

  @Value("${queueconfig.update-sample-topic}")
  private String updateSampleTopic;

  @Value("${queueconfig.update-sample-sensitive-topic}")
  private String updateSensitiveSampleTopic;

  public JobTypeSettings getJobTypeSettings(
      JobType jobType, CollectionExercise collectionExercise) {

    if (collectionExercise == null) {
      throw new RuntimeException("CollectionExercise is null!");
    }

    JobTypeSettings jobTypeSettings = new JobTypeSettings(jobType);
    switch (jobType) {
      case SAMPLE:
        jobTypeSettings.setTransformer(SAMPLE_LOAD_TRANSFORMER);
        jobTypeSettings.setColumnValidators(
            collectionExercise.getSurvey().getSampleValidationRules());
        jobTypeSettings.setTopic(toProjectTopicName(newCaseTopic, sharedPubsubProject).toString());
        jobTypeSettings.setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_SAMPLE);
        jobTypeSettings.setFileViewProgressPermission(
            UserGroupAuthorisedActivityType.VIEW_SAMPLE_LOAD_PROGRESS);
        return jobTypeSettings;

      case BULK_REFUSAL:
        jobTypeSettings.setTransformer(BULK_REFUSAL_TRANSFORMER);
        jobTypeSettings.setColumnValidators(
            getBulkRefusalProcessorValidationRules(collectionExercise));
        jobTypeSettings.setTopic(
            toProjectTopicName(refusalEventTopic, sharedPubsubProject).toString());
        jobTypeSettings.setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_BULK_REFUSAL);
        jobTypeSettings.setFileViewProgressPermission(
            UserGroupAuthorisedActivityType.VIEW_BULK_REFUSAL_PROGRESS);
        return jobTypeSettings;

      case BULK_INVALID:
        jobTypeSettings.setTransformer(BULK_INVALID_TRANSFORMER);
        jobTypeSettings.setColumnValidators(getBulkInvalidCaseValidationRules(collectionExercise));
        jobTypeSettings.setTopic(
            toProjectTopicName(invalidCaseTopic, sharedPubsubProject).toString());
        jobTypeSettings.setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_BULK_INVALID);
        jobTypeSettings.setFileViewProgressPermission(
            UserGroupAuthorisedActivityType.VIEW_BULK_INVALID_PROGRESS);
        return jobTypeSettings;

      case BULK_UPDATE_SAMPLE:
        jobTypeSettings.setTransformer(BULK_SAMPLE_UPDATE_TRANSFORMER);
        jobTypeSettings.setColumnValidators(getBulkSampleValidationRulesHeaderRowOnly());
        jobTypeSettings.setSampleAndSensitiveDataColumnMaps(
            collectionExercise.getSurvey().getSampleValidationRules(), collectionExercise);
        jobTypeSettings.setTopic(
            toProjectTopicName(updateSampleTopic, sharedPubsubProject).toString());
        jobTypeSettings.setFileLoadPermission(
            UserGroupAuthorisedActivityType.LOAD_BULK_UPDATE_SAMPLE);
        jobTypeSettings.setFileViewProgressPermission(
            UserGroupAuthorisedActivityType.VIEW_BULK_UPDATE_SAMPLE_PROGRESS);

        return jobTypeSettings;

      case BULK_UPDATE_SAMPLE_SENSITIVE:
        jobTypeSettings.setTransformer(BULK_SENSITIVE_UPDATE_TRANSFORMER);
        jobTypeSettings.setColumnValidators(getBulkSampleValidationRulesHeaderRowOnly());
        jobTypeSettings.setSampleAndSensitiveDataColumnMaps(
            collectionExercise.getSurvey().getSampleValidationRules(), collectionExercise);
        jobTypeSettings.setTopic(
            toProjectTopicName(updateSensitiveSampleTopic, sharedPubsubProject).toString());
        jobTypeSettings.setFileLoadPermission(
            UserGroupAuthorisedActivityType.LOAD_BULK_UPDATE_SAMPLE_SENSITIVE);
        jobTypeSettings.setFileViewProgressPermission(
            UserGroupAuthorisedActivityType.VIEW_BULK_UPDATE_SAMPLE_SENSITIVE_PROGRESS);

        return jobTypeSettings;

      default:
        // This code should be unreachable, providing we have a case for every JobType
        throw new RuntimeException(
            String.format("In getJobTypeSettings the jobType %s wasn't matched", jobType));
    }
  }

  private ColumnValidator[] getBulkSampleValidationRulesHeaderRowOnly() {
    return new ColumnValidator[] {
      new ColumnValidator("caseId", false, new Rule[0]),
      new ColumnValidator("fieldToUpdate", false, new Rule[0]),
      new ColumnValidator("newValue", false, new Rule[0])
    };
  }

  private ColumnValidator[] getBulkInvalidCaseValidationRules(
      CollectionExercise collectionExercise) {
    Rule[] caseExistsRules = {new CaseExistsInCollectionExerciseRule(collectionExercise)};
    ColumnValidator caseExistsValidator = new ColumnValidator("caseId", false, caseExistsRules);

    Rule[] reasonRule = {new MandatoryRule()};
    ColumnValidator reasonRuleValidator = new ColumnValidator("reason", false, reasonRule);

    return new ColumnValidator[] {caseExistsValidator, reasonRuleValidator};
  }

  private ColumnValidator[] getBulkRefusalProcessorValidationRules(
      CollectionExercise collectionExercise) {
    Rule[] caseExistsRules = {new CaseExistsInCollectionExerciseRule(collectionExercise)};
    ColumnValidator caseExistsValidator = new ColumnValidator("caseId", false, caseExistsRules);

    String[] refusalTypes =
        EnumSet.allOf(RefusalTypeDTO.class).stream().map(Enum::toString).toArray(String[]::new);
    Rule[] refusalSetRules = {new InSetRule(refusalTypes)};

    ColumnValidator refusalTypeValidator =
        new ColumnValidator("refusalType", false, refusalSetRules);

    return new ColumnValidator[] {caseExistsValidator, refusalTypeValidator};
  }
}
