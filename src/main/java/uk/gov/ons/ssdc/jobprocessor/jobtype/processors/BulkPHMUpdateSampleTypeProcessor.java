package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.common.validation.*;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkPHMUpdateSampleTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsFromParticipantIdRule;

public class BulkPHMUpdateSampleTypeProcessor extends JobTypeProcessor {
  private static final Transformer BULK_PHM_SAMPLE_UPDATE_TRANSFORMER =
      new BulkPHMUpdateSampleTransformer();

  public BulkPHMUpdateSampleTypeProcessor(
      String topic, String sharedPubsubProject, CollectionExercise collectionExercise) {
    setJobType(JobType.BULK_PHM_UPDATE_SAMPLE);
    setTransformer(BULK_PHM_SAMPLE_UPDATE_TRANSFORMER);

    setColumnValidators(getBulkPHMUpdateSameValidationRules(collectionExercise));
    setSampleAndSensitiveDataColumnMaps(
        collectionExercise.getSurvey().getSampleValidationRules(), collectionExercise);
    setTopic(toProjectTopicName(topic, sharedPubsubProject).toString());
    setFileLoadPermission(UserGroupAuthorisedActivityType.LOAD_BULK_PHM_UPDATE_SAMPLE);
    setFileViewProgressPermission(
        UserGroupAuthorisedActivityType.VIEW_BULK_PHM_UPDATE_SAMPLE_PROGRESS);
  }

  private ColumnValidator[] getBulkPHMUpdateSameValidationRules(
      CollectionExercise collectionExercise) {

    // PARTICIPANT_ID
    Rule[] participantRules = {
      new LengthRule(16),
      new MandatoryRule(),
      new CaseExistsFromParticipantIdRule(collectionExercise)
    };

    ColumnValidator participantIdValidator =
        new ColumnValidator("PARTICIPANT_ID", false, participantRules);

    // SWAB_TEST_BARCODE
    ColumnValidator swabTestBarcodeValidator =
        new ColumnValidator("SWAB_TEST_BARCODE", false, new Rule[0]);

    // BLOOD_TEST_BARCODE
    ColumnValidator bloodTestBarcodeValidator =
        new ColumnValidator("BLOOD_TEST_BARCODE", false, new Rule[0]);

    // NO_TEST_BARCODE
    ColumnValidator noTestBarcodeValidator =
        new ColumnValidator("NO_TEST_BARCODE", false, new Rule[0]);

    // COHORT_TYPE
    String[] cohortTypeSet = {"Q", "S", "B"};
    Rule[] cohortTypeRules = {new InSetRule(cohortTypeSet)};
    ColumnValidator cohortTypeValidator =
        new ColumnValidator("COHORT_TYPE", false, cohortTypeRules);

    // BATCH_OPEN_DATE
    Rule[] batchOpenRules = {new MandatoryRule()};
    ColumnValidator batchOpenDateValidator =
        new ColumnValidator("BATCH_OPEN_DATE", false, batchOpenRules);

    // BATCH_CLOSE_DATE
    Rule[] batchCloseRules = {new MandatoryRule()};
    ColumnValidator batchCloseDateValidator =
        new ColumnValidator("BATCH_CLOSE_DATE", false, batchCloseRules);

    // BATCH_NUMBER
    Rule[] batchNumberRules = {new NumericRule(), new MandatoryRule()};
    ColumnValidator batchNumberValidator =
        new ColumnValidator("BATCH_NUMBER", false, batchNumberRules);

    // LONGITUDINAL_QUESTIONS
    String[] longitudinalQuestionsSet = {"T", "F"};
    Rule[] longitudinalQuestionsRules = {
      new MandatoryRule(), new InSetRule(longitudinalQuestionsSet), new LengthRule(1)
    };
    ColumnValidator longitudinalQuestionsValidator =
        new ColumnValidator("LONGITUDINAL_QUESTIONS", false, longitudinalQuestionsRules);

    return new ColumnValidator[] {
      participantIdValidator,
      swabTestBarcodeValidator,
      bloodTestBarcodeValidator,
      noTestBarcodeValidator,
      cohortTypeValidator,
      batchOpenDateValidator,
      batchCloseDateValidator,
      batchNumberValidator,
      longitudinalQuestionsValidator
    };
  }

  @Override
  public ColumnValidator[] getColumnValidators(JobRow jobRow) {
    return columnValidators.clone();
  }
}
