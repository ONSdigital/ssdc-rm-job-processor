package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.common.validation.InSetRule;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsInCollectionExerciseRule;

public class SampleAndSensitiveSampleHelper {

  public static Map<String, ColumnValidator[]> getSampleAndSensitiveDataColumnMaps(
      ColumnValidator[] columnValidators, CollectionExercise collectionExercise, JobType jobType) {

    boolean jobSensitive = jobType == JobType.BULK_UPDATE_SAMPLE_SENSITIVE;

    Map<String, ColumnValidator[]> sampleOrSensitiveValidationsMap = new HashMap<>();
    String[] allValidColumns =
        Arrays.stream(columnValidators)
            .filter(columnValidator -> columnValidator.isSensitive() == jobSensitive)
            .map(ColumnValidator::getColumnName)
            .toArray(String[]::new);

    for (ColumnValidator columnValidator : columnValidators) {
      if (jobSensitive == columnValidator.isSensitive()) {
        sampleOrSensitiveValidationsMap.put(
            columnValidator.getColumnName(),
            createColumnValidation(
                allValidColumns, columnValidator.getRules(), collectionExercise));
      }
    }

    return sampleOrSensitiveValidationsMap;
  }

  private static ColumnValidator[] createColumnValidation(
      String[] allowedColumns, Rule[] newValueRules, CollectionExercise collectionExercise) {
    Rule[] caseExistsRules = {new CaseExistsInCollectionExerciseRule(collectionExercise)};
    ColumnValidator caseExistsValidator = new ColumnValidator("caseId", false, caseExistsRules);

    Rule[] fieldToUpdateRule = {new InSetRule(allowedColumns)};
    ColumnValidator fieldToUpdateValidator =
        new ColumnValidator("fieldToUpdate", false, fieldToUpdateRule);

    ColumnValidator newValueValidator = new ColumnValidator("newValue", false, newValueRules);

    return new ColumnValidator[] {caseExistsValidator, fieldToUpdateValidator, newValueValidator};
  }

  public static ColumnValidator[] getColumnValidatorForSampleOrSensitiveDataRows(
      Map<String, ColumnValidator[]> sampleOrSensitiveValidationsMap, String columnName)
      throws ValidatorFieldNotFoundException {
    if (!sampleOrSensitiveValidationsMap.containsKey(columnName)) {
      throw new ValidatorFieldNotFoundException(
          "fieldToUpdate column " + columnName + " does not exist");
    }

    return sampleOrSensitiveValidationsMap.get(columnName);
  }

  public static ColumnValidator[] getBulkSampleValidationRulesHeaderRowOnly() {
    return new ColumnValidator[] {
      new ColumnValidator("caseId", false, new Rule[0]),
      new ColumnValidator("fieldToUpdate", false, new Rule[0]),
      new ColumnValidator("newValue", false, new Rule[0])
    };
  }
}
