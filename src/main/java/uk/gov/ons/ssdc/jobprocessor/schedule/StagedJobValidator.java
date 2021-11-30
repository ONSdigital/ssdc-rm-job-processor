package uk.gov.ons.ssdc.jobprocessor.schedule;

import java.util.List;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.common.model.entity.JobStatus;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;

@Component
public class StagedJobValidator {

  private final JobRepository jobRepository;
  private final JobRowRepository jobRowRepository;
  private final RowChunkValidator rowChunkValidator;

  public StagedJobValidator(
      JobRepository jobRepository,
      JobRowRepository jobRowRepository,
      RowChunkValidator rowChunkValidator) {
    this.jobRepository = jobRepository;
    this.jobRowRepository = jobRowRepository;
    this.rowChunkValidator = rowChunkValidator;
  }

  @Scheduled(fixedDelayString = "1000")
  @Transactional
  public void processStagedJobs() {
    List<Job> jobs = jobRepository.findByJobStatus(JobStatus.VALIDATION_IN_PROGRESS);

    for (Job job : jobs) {
      JobStatus jobStatus = JobStatus.VALIDATED_OK;

      while (jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.STAGED)) {
        if (rowChunkValidator.processChunk(job)) {
          jobStatus = JobStatus.VALIDATED_WITH_ERRORS;
        }

        if (jobStatus == JobStatus.VALIDATED_TOTAL_FAILURE) {
          break;
        }
      }

      if (jobStatus == JobStatus.VALIDATED_TOTAL_FAILURE) {
        jobRowRepository.deleteByJob(job);
      }

      job.setJobStatus(jobStatus);
      jobRepository.saveAndFlush(job);
    }
  }
}
