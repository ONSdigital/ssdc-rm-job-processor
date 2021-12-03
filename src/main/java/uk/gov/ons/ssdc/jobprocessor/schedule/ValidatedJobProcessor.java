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
public class ValidatedJobProcessor {
  private final JobRepository jobRepository;
  private final JobRowRepository jobRowRepository;
  private final RowChunkProcessor rowChunkProcessor;

  public ValidatedJobProcessor(
      JobRepository jobRepository,
      JobRowRepository jobRowRepository,
      RowChunkProcessor rowChunkProcessor) {
    this.jobRepository = jobRepository;
    this.jobRowRepository = jobRowRepository;
    this.rowChunkProcessor = rowChunkProcessor;
  }

  @Scheduled(fixedDelayString = "1000")
  @Transactional
  public void processStagedJobs() {
    List<Job> jobs = jobRepository.findByJobStatus(JobStatus.PROCESSING_IN_PROGRESS);

    for (Job job : jobs) {
      while (jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.VALIDATED_OK)) {
        rowChunkProcessor.processChunk(job);
      }

      job.setJobStatus(JobStatus.PROCESSED);
      jobRepository.save(job);

      jobRowRepository.deleteByJobAndJobRowStatus(job, JobRowStatus.PROCESSED);
    }
  }
}
