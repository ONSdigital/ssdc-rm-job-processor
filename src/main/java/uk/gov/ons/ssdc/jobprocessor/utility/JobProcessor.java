package uk.gov.ons.ssdc.jobprocessor.utility;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.UserGroupAuthorisedActivityType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.common.validation.InSetRule;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;
import uk.gov.ons.ssdc.jobprocessor.validators.CaseExistsInCollectionExerciseRule;

@Data
public abstract class JobProcessor {
  private JobType jobType;
  private Transformer transformer;
  private ColumnValidator[] columnValidators;
  private String topic;
  private UserGroupAuthorisedActivityType fileLoadPermission;
  private UserGroupAuthorisedActivityType fileViewProgressPermission;
  private Map<String, ColumnValidator[]> sampleOrSensitiveValidationsMap;
  private boolean fixedColumnValidators;
  private boolean blankValueReturnNoValidators;

  public JobProcessor() {};

  public ColumnValidator[] getColumnValidators(JobRow jobRow)
      throws ValidatorFieldNotFoundException {
    return columnValidators;
  }

  public void setSampleAndSensitiveDataColumnMaps(
      ColumnValidator[] columnValidators, CollectionExercise collectionExercise) {

    boolean jobSensitive = jobType == JobType.BULK_UPDATE_SAMPLE_SENSITIVE;

    sampleOrSensitiveValidationsMap = new HashMap<>();
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
  }

  private ColumnValidator[] createColumnValidation(
      String[] allowedColumns, Rule[] newValueRules, CollectionExercise collectionExercise) {
    Rule[] caseExistsRules = {new CaseExistsInCollectionExerciseRule(collectionExercise)};
    ColumnValidator caseExistsValidator = new ColumnValidator("caseId", false, caseExistsRules);

    Rule[] fieldToUpdateRule = {new InSetRule(allowedColumns)};
    ColumnValidator fieldToUpdateValidator =
        new ColumnValidator("fieldToUpdate", false, fieldToUpdateRule);

    ColumnValidator newValueValidator = new ColumnValidator("newValue", false, newValueRules);

    return new ColumnValidator[] {caseExistsValidator, fieldToUpdateValidator, newValueValidator};
  }

  protected ColumnValidator[] getColumnValidatorForSampleOrSensitiveDataRows(String columnName)
      throws ValidatorFieldNotFoundException {
    if (!sampleOrSensitiveValidationsMap.containsKey(columnName)) {
      throw new ValidatorFieldNotFoundException(
          "fieldToUpdate column %s does not exist: " + columnName);
    }

    return sampleOrSensitiveValidationsMap.get(columnName);
  }

  protected ColumnValidator[] getBulkSampleValidationRulesHeaderRowOnly() {
    return new ColumnValidator[] {
      new ColumnValidator("caseId", false, new Rule[0]),
      new ColumnValidator("fieldToUpdate", false, new Rule[0]),
      new ColumnValidator("newValue", false, new Rule[0])
    };
  }
}
