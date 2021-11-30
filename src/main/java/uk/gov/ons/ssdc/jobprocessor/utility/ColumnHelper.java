package uk.gov.ons.ssdc.jobprocessor.utility;

import uk.gov.ons.ssdc.common.validation.ColumnValidator;

public class ColumnHelper {
  public static String[] getExpectedColumns(ColumnValidator[] validationRules) {
    String[] expectedColumns = new String[validationRules.length];

    for (int i = 0; i < expectedColumns.length; i++) {
      expectedColumns[i] = validationRules[i].getColumnName();
    }

    return expectedColumns;
  }
}
