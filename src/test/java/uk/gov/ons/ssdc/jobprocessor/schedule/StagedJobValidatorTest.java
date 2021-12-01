package uk.gov.ons.ssdc.jobprocessor.schedule;

import static org.assertj.core.api.Assertions.assertThat;
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
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.common.model.entity.JobStatus;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;

@ExtendWith(MockitoExtension.class)
class StagedJobValidatorTest {
  @Mock JobRepository jobRepository;
  @Mock JobRowRepository jobRowRepository;
  @Mock RowChunkValidator rowChunkValidator;

  @InjectMocks StagedJobValidator underTest;

  @Test
  void processStagedJobs() {
    // Given
    Job job = new Job();

    when(jobRepository.findByJobStatus(JobStatus.VALIDATION_IN_PROGRESS)).thenReturn(List.of(job));
    when(jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(true)
        .thenReturn(false);
    when(rowChunkValidator.processChunk(job)).thenReturn(false);

    // When
    underTest.processStagedJobs();

    // Then
    verify(rowChunkValidator).processChunk(job);

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();
    assertThat(actualJob.getJobStatus()).isEqualTo(JobStatus.VALIDATED_OK);
  }

  @Test
  void processStagedJobsMultipleChunks() {
    // Given
    Job job = new Job();

    when(jobRepository.findByJobStatus(JobStatus.VALIDATION_IN_PROGRESS)).thenReturn(List.of(job));
    when(jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);
    when(rowChunkValidator.processChunk(job)).thenReturn(false);

    // When
    underTest.processStagedJobs();

    // Then
    verify(rowChunkValidator, times(3)).processChunk(job);

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();
    assertThat(actualJob.getJobStatus()).isEqualTo(JobStatus.VALIDATED_OK);
  }

  @Test
  void processStagedJobsFailedChunk() {
    // Given
    Job job = new Job();

    when(jobRepository.findByJobStatus(JobStatus.VALIDATION_IN_PROGRESS)).thenReturn(List.of(job));
    when(jobRowRepository.existsByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(true)
        .thenReturn(false);
    when(rowChunkValidator.processChunk(job)).thenReturn(true);

    // When
    underTest.processStagedJobs();

    // Then
    verify(rowChunkValidator).processChunk(job);

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();
    assertThat(actualJob.getJobStatus()).isEqualTo(JobStatus.VALIDATED_WITH_ERRORS);
  }
}
