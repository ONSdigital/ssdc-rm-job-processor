package uk.gov.ons.ssdc.jobprocessor.transformer;

import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;

public interface Transformer {
  Object transformRow(Job job, JobRow jobRow, ColumnValidator[] columnValidators, String topic);
}
