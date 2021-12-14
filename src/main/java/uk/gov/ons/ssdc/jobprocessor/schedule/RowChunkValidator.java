package uk.gov.ons.ssdc.jobprocessor.schedule;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;
import uk.gov.ons.ssdc.jobprocessor.utility.JobTypeHelper;
import uk.gov.ons.ssdc.jobprocessor.utility.JobTypeSettings;

@Component
public class RowChunkValidator {
  private final JobRowRepository jobRowRepository;
  private final JobRepository jobRepository;
  private final JobTypeHelper jobTypeHelper;

  public RowChunkValidator(
      JobRowRepository jobRowRepository, JobRepository jobRepository, JobTypeHelper jobTypeHelper) {
    this.jobRowRepository = jobRowRepository;
    this.jobRepository = jobRepository;
    this.jobTypeHelper = jobTypeHelper;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void processChunk(Job job) {
    JobTypeSettings jobTypeSettings =
        jobTypeHelper.getJobTypeSettings(job.getJobType(), job.getCollectionExercise());

    List<JobRow> jobRows =
        jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED);

    boolean getValidationRulesPerRow = false;
    ColumnValidator[] columnValidators = null;

    if (job.getJobType() == JobType.BULK_UPDATE_SAMPLE
        || job.getJobType() == JobType.BULK_UPDATE_SAMPLE_SENSITIVE) {
      getValidationRulesPerRow = true;
    } else {
      columnValidators = jobTypeSettings.getColumnValidators();
    }

    for (JobRow jobRow : jobRows) {
      JobRowStatus rowStatus = JobRowStatus.VALIDATED_OK;
      List<String> rowValidationErrors = new LinkedList<>();

      if (getValidationRulesPerRow) {
        // If it's a sensitive data update, and the row is empty then it means "blank out the data"
        if (job.getJobType() == JobType.BULK_UPDATE_SAMPLE_SENSITIVE
            && "".equals(jobRow.getRowData().get("newValue"))) {
          // We should disregard all validation rules if we're blanking out the data
          columnValidators = new ColumnValidator[0];
        } else {
          String fieldToUpdate = jobRow.getRowData().get("fieldToUpdate");
          columnValidators =
              jobTypeSettings.getColumnValidatorForSampleOrSensitiveDataRows(fieldToUpdate);

          if (columnValidators == null) {
            rowStatus = JobRowStatus.VALIDATED_ERROR;
            rowValidationErrors.add(
                String.format("fieldToUpdate column %s does not exist", fieldToUpdate));
            columnValidators = new ColumnValidator[0];
          }
        }
      }

      for (ColumnValidator columnValidator : columnValidators) {
        Optional<String> columnValidationErrors = columnValidator.validateRow(jobRow.getRowData());

        if (columnValidationErrors.isPresent()) {
          rowStatus = JobRowStatus.VALIDATED_ERROR;
          rowValidationErrors.add(columnValidationErrors.get());
        }
      }

      if (rowStatus == JobRowStatus.VALIDATED_ERROR) {
        job.setErrorRowCount(job.getErrorRowCount() + 1);
      }

      jobRow.setValidationErrorDescriptions(String.join(", ", rowValidationErrors));
      jobRow.setJobRowStatus(rowStatus);
      job.setValidatingRowNumber(job.getValidatingRowNumber() + 1);
    }

    jobRowRepository.saveAll(jobRows);
    jobRepository.saveAndFlush(job);
  }
}
