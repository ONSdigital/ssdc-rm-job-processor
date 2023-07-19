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
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.jobprocessor.exceptions.ValidatorFieldNotFoundException;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.JobTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;
import uk.gov.ons.ssdc.jobprocessor.utility.JobTypeHelper;

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
    JobTypeProcessor jobTypeProcessor =
        jobTypeHelper.getJobTypeProcessor(job.getJobType(), job.getCollectionExercise());

    List<JobRow> jobRows =
        jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED);

    for (JobRow jobRow : jobRows) {
      JobRowStatus rowStatus = JobRowStatus.VALIDATED_OK;
      List<String> rowValidationErrors = new LinkedList<>();
      ColumnValidator[] columnValidators;

      try {
        columnValidators = jobTypeProcessor.getColumnValidators(jobRow);
      } catch (ValidatorFieldNotFoundException ex) {
        rowStatus = JobRowStatus.VALIDATED_ERROR;
        rowValidationErrors.add(ex.getMessage());
        columnValidators = new ColumnValidator[0];
      }

      for (ColumnValidator columnValidator : columnValidators) {
        try {
          Optional<String> columnValidationErrors =
              columnValidator.validateRow(jobRow.getRowData());

          if (columnValidationErrors.isPresent()) {
            rowStatus = JobRowStatus.VALIDATED_ERROR;
            rowValidationErrors.add(columnValidationErrors.get());
          }

        } catch (IllegalArgumentException ex) {
          rowStatus = JobRowStatus.VALIDATED_ERROR;
          rowValidationErrors.add("Error on cell: " + jobRow.getRowData());
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
