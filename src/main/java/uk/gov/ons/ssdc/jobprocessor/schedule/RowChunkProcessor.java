package uk.gov.ons.ssdc.jobprocessor.schedule;

import com.godaddy.logging.Logger;
import com.godaddy.logging.LoggerFactory;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.concurrent.ListenableFuture;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;
import uk.gov.ons.ssdc.jobprocessor.utility.JobTypeHelper;
import uk.gov.ons.ssdc.jobprocessor.utility.JobTypeSettings;

@Component
public class RowChunkProcessor {
  private static final Logger log = LoggerFactory.getLogger(RowChunkProcessor.class);
  private final JobRowRepository jobRowRepository;
  private final PubSubTemplate pubSubTemplate;
  private final JobRepository jobRepository;
  private final JobTypeHelper jobTypeHelper;

  public RowChunkProcessor(
      JobRowRepository jobRowRepository,
      PubSubTemplate pubSubTemplate,
      JobRepository jobRepository,
      JobTypeHelper jobTypeHelper) {
    this.jobRowRepository = jobRowRepository;
    this.pubSubTemplate = pubSubTemplate;
    this.jobRepository = jobRepository;
    this.jobTypeHelper = jobTypeHelper;
  }

  @Transactional(propagation = Propagation.REQUIRES_NEW)
  public void processChunk(Job job) {
    JobTypeSettings jobTypeSettings =
        jobTypeHelper.getJobTypeSettings(job.getJobType(), job.getCollectionExercise());

    List<JobRow> jobRows =
        jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.VALIDATED_OK);

    for (JobRow jobRow : jobRows) {
      try {
        ListenableFuture<String> future =
            pubSubTemplate.publish(
                jobTypeSettings.getTopic(),
                jobTypeSettings
                    .getTransformer()
                    .transformRow(
                        job,
                        jobRow,
                        jobTypeSettings.getColumnValidators(),
                        jobTypeSettings.getTopic()));

        // Wait for up to 30 seconds to confirm that message was published
        future.get(30, TimeUnit.SECONDS);

        jobRow.setJobRowStatus(JobRowStatus.PROCESSED);
        job.setProcessingRowNumber(job.getProcessingRowNumber() + 1);
      } catch (Exception e) {
        // The message sending will be retried...
        log.with("job ID", job.getId())
            .with("row ID", jobRow.getId())
            .error("Failed to send message to pubsub", e);
      }
    }

    jobRepository.save(job);
    jobRowRepository.saveAll(jobRows);
  }
}
