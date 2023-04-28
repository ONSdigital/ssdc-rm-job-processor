package uk.gov.ons.ssdc.jobprocessor.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.common.model.entity.JobStatus;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;

@ExtendWith(MockitoExtension.class)
class ValidatedJobProcessorTest {
  @Mock JobRepository jobRepository;
  @Mock JobRowRepository jobRowRepository;
  @Mock RowChunkProcessor rowChunkProcessor;

  @InjectMocks ValidatedJobProcessor underTest;

  @Test
  void processStagedJobs() {
    // Given
    Job job = new Job();
    when(jobRepository.findByJobStatus(JobStatus.PROCESSING_IN_PROGRESS)).thenReturn(List.of(job));
    when(jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.VALIDATED_OK))
        .thenReturn(true)
        .thenReturn(false);

    // When
    underTest.processStagedJobs();

    // Then
    verify(rowChunkProcessor).processChunk(job);

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).save(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();
    assertThat(actualJob.getJobStatus()).isEqualTo(JobStatus.PROCESSED);

    verify(jobRowRepository).deleteByJobAndJobRowStatus(job, JobRowStatus.PROCESSED);
  }

  @Test
  void processStagedJobsMultipleChunks() {
    // Given
    Job job = new Job();
    when(jobRepository.findByJobStatus(JobStatus.PROCESSING_IN_PROGRESS)).thenReturn(List.of(job));
    when(jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.VALIDATED_OK))
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    // When
    underTest.processStagedJobs();

    // Then
    verify(rowChunkProcessor, times(3)).processChunk(job);

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).save(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();
    assertThat(actualJob.getJobStatus()).isEqualTo(JobStatus.PROCESSED);

    verify(jobRowRepository).deleteByJobAndJobRowStatus(job, JobRowStatus.PROCESSED);
  }

  @Test
  void processStagedJobsMultipleJobs() {
    // Given
    when(jobRepository.findByJobStatus(JobStatus.PROCESSING_IN_PROGRESS))
        .thenReturn(List.of(new Job(), new Job(), new Job()));
    when(jobRowRepository.existsByJobAndJobRowStatus(any(Job.class), eq(JobRowStatus.VALIDATED_OK)))
        .thenReturn(true)
        .thenReturn(false)
        .thenReturn(true)
        .thenReturn(false)
        .thenReturn(true)
        .thenReturn(false);

    // When
    underTest.processStagedJobs();

    // Then
    verify(rowChunkProcessor, times(3)).processChunk(any(Job.class));
    verify(jobRepository, times(3)).save(any(Job.class));
    verify(jobRowRepository, times(3))
        .deleteByJobAndJobRowStatus(any(Job.class), eq(JobRowStatus.PROCESSED));
  }

  @Test
  void removeCancelledJobRow() {

    // Given
    List<JobRow> jobRows = List.of(new JobRow());
    when(jobRowRepository.findTop500ByJob_JobStatus(JobStatus.CANCELLED)).thenReturn(jobRows);

    // When
    underTest.removeCancelledJobsRows();

    // Then
    verify(jobRowRepository).deleteAll(jobRows);
  }
}
