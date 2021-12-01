package uk.gov.ons.ssdc.jobprocessor.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.concurrent.ListenableFuture;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;
import uk.gov.ons.ssdc.jobprocessor.transformer.Transformer;
import uk.gov.ons.ssdc.jobprocessor.utility.JobTypeHelper;
import uk.gov.ons.ssdc.jobprocessor.utility.JobTypeSettings;

@ExtendWith(MockitoExtension.class)
class RowChunkProcessorTest {
  @Mock JobRowRepository jobRowRepository;
  @Mock PubSubTemplate pubSubTemplate;
  @Mock JobRepository jobRepository;
  @Mock JobTypeHelper jobTypeHelper;

  @InjectMocks RowChunkProcessor underTest;

  @Test
  void processChunk() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.SAMPLE);
    job.setCollectionExercise(collectionExercise);

    Transformer transformer = mock(Transformer.class);
    ColumnValidator[] columnValidators =
        new ColumnValidator[] {new ColumnValidator("test column", false, new Rule[0])};

    JobTypeSettings jobTypeSettings = new JobTypeSettings(JobType.SAMPLE);
    jobTypeSettings.setTopic("Test topic");
    jobTypeSettings.setTransformer(transformer);
    jobTypeSettings.setColumnValidators(columnValidators);

    JobRow jobRow = new JobRow();
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeSettings(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeSettings);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.VALIDATED_OK))
        .thenReturn(jobRows);

    Object messageToPublish = new Object();
    when(transformer.transformRow(job, jobRow, columnValidators, jobTypeSettings.getTopic()))
        .thenReturn(messageToPublish);

    ListenableFuture<String> listenableFuture = mock(ListenableFuture.class);
    when(pubSubTemplate.publish(jobTypeSettings.getTopic(), messageToPublish))
        .thenReturn(listenableFuture);

    // When
    underTest.processChunk(job);

    // Then
    verify(pubSubTemplate).publish(jobTypeSettings.getTopic(), messageToPublish);

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).save(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getProcessingRowNumber()).isEqualTo(1);

    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.PROCESSED);
  }

  @Test
  void processChunkRowFailsToSend() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.SAMPLE);
    job.setCollectionExercise(collectionExercise);

    Transformer transformer = mock(Transformer.class);
    ColumnValidator[] columnValidators =
        new ColumnValidator[] {new ColumnValidator("test column", false, new Rule[0])};

    JobTypeSettings jobTypeSettings = new JobTypeSettings(JobType.SAMPLE);
    jobTypeSettings.setTopic("Test topic");
    jobTypeSettings.setTransformer(transformer);
    jobTypeSettings.setColumnValidators(columnValidators);

    JobRow jobRow = new JobRow();
    jobRow.setJobRowStatus(JobRowStatus.VALIDATED_OK);
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeSettings(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeSettings);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.VALIDATED_OK))
        .thenReturn(jobRows);

    Object messageToPublish = new Object();
    when(transformer.transformRow(job, jobRow, columnValidators, jobTypeSettings.getTopic()))
        .thenReturn(messageToPublish);

    when(pubSubTemplate.publish(jobTypeSettings.getTopic(), messageToPublish))
        .thenThrow(new RuntimeException());

    // When
    underTest.processChunk(job);

    // Then
    verify(pubSubTemplate).publish(jobTypeSettings.getTopic(), messageToPublish);

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).save(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getProcessingRowNumber()).isEqualTo(0);

    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_OK);
  }

  @Test
  void processChunkRowTransformFails() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.SAMPLE);
    job.setCollectionExercise(collectionExercise);

    Transformer transformer = mock(Transformer.class);
    ColumnValidator[] columnValidators =
        new ColumnValidator[] {new ColumnValidator("test column", false, new Rule[0])};

    JobTypeSettings jobTypeSettings = new JobTypeSettings(JobType.SAMPLE);
    jobTypeSettings.setTopic("Test topic");
    jobTypeSettings.setTransformer(transformer);
    jobTypeSettings.setColumnValidators(columnValidators);

    JobRow jobRow = new JobRow();
    jobRow.setJobRowStatus(JobRowStatus.VALIDATED_OK);
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeSettings(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeSettings);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.VALIDATED_OK))
        .thenReturn(jobRows);

    when(transformer.transformRow(job, jobRow, columnValidators, jobTypeSettings.getTopic()))
        .thenThrow(new RuntimeException());

    // When
    underTest.processChunk(job);

    // Then
    verify(pubSubTemplate, never()).publish(any(), any());

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).save(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getProcessingRowNumber()).isEqualTo(0);

    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_OK);
  }
}
