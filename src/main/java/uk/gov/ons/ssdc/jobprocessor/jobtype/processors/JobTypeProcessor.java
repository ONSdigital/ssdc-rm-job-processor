package uk.gov.ons.ssdc.jobprocessor.jobtype.processors;

import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;

public interface JobTypeProcessor {
  ColumnValidator[] getColumnValidators(JobRow jobRow) throws ValidatorFieldNotFoundException;

  JobType getJobType();

  void setJobType(JobType jobType);

  Transformer getTransformer();

  void setTransformer(Transformer transformer);

  String getTopic();

  void setTopic(String topic);
}
