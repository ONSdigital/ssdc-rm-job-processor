package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.common.validation.*;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.model.dto.messaging.RefusalTypeDTO;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkPHMUpdateSampleTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.BulkUpdateSampleTransformer;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsFromParticipantIdRule;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsInCollectionExerciseRule;

import java.util.EnumSet;

import static com.google.cloud.spring.pubsub.support.PubSubTopicUtils.toProjectTopicName;

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
    setFileViewProgressPermission(UserGroupAuthorisedActivityType.VIEW_BULK_PHM_UPDATE_SAMPLE_PROGRESS);
  }

  private ColumnValidator[] getBulkPHMUpdateSameValidationRules(CollectionExercise collectionExercise) {
    Rule[] participantRules = {new NumericRule(), new MandatoryRule(), new CaseExistsFromParticipantIdRule(collectionExercise)};
    ColumnValidator participantValidator = new ColumnValidator("PARTICIPANT_ID", false, participantRules);

    ColumnValidator swabTestBarcodeValidator = new ColumnValidator("SWAB_TEST_BARCODE", false, new Rule[0]);
    ColumnValidator bloodTestBarcodeValidator =  new ColumnValidator("BLOOD_TEST_BARCODE", false, new Rule[0]);
    ColumnValidator noTestBarcodeValidator = new ColumnValidator("NO_TEST_BARCODE", false, new Rule[0]);

    // Needs inset for values. Can get elsewhere
    String[] cohortTypeSet = {"Q","S","B"};
    Rule[] cohortTypeRules = {new InSetRule(cohortTypeSet)};

    ColumnValidator cohortTypeValidator = new ColumnValidator("COHORT_TYPE", false, cohortTypeRules);

    Rule[] batchOpenRules = { new MandatoryRule()};
    ColumnValidator batchOpenDateValidator = new ColumnValidator("BATCH_OPEN_DATE", false, batchOpenRules);

    Rule[] batchCloseRules = {new MandatoryRule()};
    ColumnValidator batchCloseDateValidator = new ColumnValidator("BATCH_CLOSE_DATE", false, batchCloseRules);

    Rule[] batchNumberRules = {new NumericRule(), new MandatoryRule()};
    ColumnValidator batchNumberValidator = new ColumnValidator("BATCH_NUMBER", false, batchNumberRules);
    String[] longitudinalQuestionsSet = {"S","L"};
    Rule[] longitudinalQuestionsRules = {new MandatoryRule(), new InSetRule(longitudinalQuestionsSet)};
    ColumnValidator longitudinalQuestionsValidator = new ColumnValidator("LONGITUDINAL_QUESTIONS", false, longitudinalQuestionsRules);

    return new ColumnValidator[] {participantValidator, swabTestBarcodeValidator, bloodTestBarcodeValidator, noTestBarcodeValidator, cohortTypeValidator,
            batchOpenDateValidator, batchCloseDateValidator, batchNumberValidator, longitudinalQuestionsValidator};
  }

  @Override
  public ColumnValidator[] getColumnValidators(JobRow jobRow) {
    return columnValidators.clone();
  }
}
