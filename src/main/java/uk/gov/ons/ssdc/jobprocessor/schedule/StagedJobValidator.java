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

      while (jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.STAGED)) {
        rowChunkValidator.processChunk(job);
      }

      JobStatus jobStatus = JobStatus.VALIDATED_OK;
      if (jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.VALIDATED_ERROR)) {
        jobStatus = JobStatus.VALIDATED_WITH_ERRORS;
      }

      job.setJobStatus(jobStatus);
      jobRepository.saveAndFlush(job);
    }
  }
}
