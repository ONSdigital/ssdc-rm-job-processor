package uk.gov.ons.ssdc.jobprocessor.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.common.model.entity.JobType;
import uk.gov.ons.ssdc.common.model.entity.Survey;
import uk.gov.ons.ssdc.common.validation.ColumnValidator;
import uk.gov.ons.ssdc.common.validation.LengthRule;
import uk.gov.ons.ssdc.common.validation.MandatoryRule;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.BulkUpdateSampleTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.BulkUpdateSensitiveSampleTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.JobTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.jobtype.processors.SampleLoadTypeProcessor;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRepository;
import uk.gov.ons.ssdc.jobprocessor.repository.JobRowRepository;
import uk.gov.ons.ssdc.jobprocessor.utility.JobTypeHelper;

@ExtendWith(MockitoExtension.class)
class RowChunkValidatorTest {
  @Mock JobRowRepository jobRowRepository;
  @Mock JobRepository jobRepository;
  @Mock JobTypeHelper jobTypeHelper;

  @InjectMocks RowChunkValidator underTest;

  @Test
  void processChunk() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.SAMPLE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("test column", false, new Rule[] {new MandatoryRule()})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor = new SampleLoadTypeProcessor("", "", collectionExercise);

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("test column", "test data"));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_OK);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions()).isEmpty();

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(0);
  }

  @Test
  void processChunkFailsValidation() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.SAMPLE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("test column", false, new Rule[] {new MandatoryRule()})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor = new SampleLoadTypeProcessor("", "", collectionExercise);

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("test column", ""));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_ERROR);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions())
        .isEqualTo("Column 'test column' value '' validation error: Mandatory value missing");

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(1);
  }

  @Test
  void processChunkBulkUpdateSample() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.BULK_UPDATE_SAMPLE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("newValue", false, new Rule[] {new MandatoryRule()})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor =
        new BulkUpdateSampleTypeProcessor("", "", collectionExercise);
    jobTypeProcessor.setSampleOrSensitiveValidationsMap(Map.of("test column", columnValidators));

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("fieldToUpdate", "test column", "newValue", "test data"));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_OK);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions()).isEmpty();

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(0);
  }

  @Test
  void processChunkBulkUpdateSampleFailsValidation() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.BULK_UPDATE_SAMPLE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("newValue", false, new Rule[] {new MandatoryRule()})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor =
        new BulkUpdateSampleTypeProcessor("", "", collectionExercise);
    jobTypeProcessor.setSampleOrSensitiveValidationsMap(Map.of("test column", columnValidators));

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("fieldToUpdate", "test column", "newValue", ""));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_ERROR);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions())
        .isEqualTo("Column 'newValue' value '' validation error: Mandatory value missing");

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(1);
  }

  @Test
  void processChunkBulkUpdateSampleUnknownColumn() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.BULK_UPDATE_SAMPLE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("newValue", false, new Rule[] {new MandatoryRule()})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor =
        new BulkUpdateSampleTypeProcessor("", "", collectionExercise);
    jobTypeProcessor.setSampleOrSensitiveValidationsMap(Map.of("test column", columnValidators));

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("fieldToUpdate", "nonexistent column", "newValue", "test data"));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_ERROR);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions())
        .isEqualTo("fieldToUpdate column nonexistent column does not exist");

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(1);
  }

  @Test
  void processChunkBulkUpdateSampleSensitive() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.BULK_UPDATE_SAMPLE_SENSITIVE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("newValue", true, new Rule[] {new MandatoryRule()})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor =
        new BulkUpdateSensitiveSampleTypeProcessor("", "", collectionExercise);
    jobTypeProcessor.setSampleOrSensitiveValidationsMap(Map.of("test column", columnValidators));

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("fieldToUpdate", "test column", "newValue", "test data"));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_OK);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions()).isEmpty();

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(0);
  }

  @Test
  void processChunkBulkUpdateSampleSensitiveFailsValidation() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.BULK_UPDATE_SAMPLE_SENSITIVE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("newValue", false, new Rule[] {new LengthRule(5)})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor =
        new BulkUpdateSensitiveSampleTypeProcessor("", "", collectionExercise);
    jobTypeProcessor.setSampleOrSensitiveValidationsMap(Map.of("test column", columnValidators));

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("fieldToUpdate", "test column", "newValue", "123456789"));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_ERROR);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions())
        .isEqualTo(
            "Column 'newValue' value '123456789' validation error: Exceeded max length of 5");

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(1);
  }

  @Test
  void processChunkBulkUpdateSampleSensitiveBlankingAllowed() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.BULK_UPDATE_SAMPLE_SENSITIVE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("newValue", true, new Rule[] {new MandatoryRule()})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor =
        new BulkUpdateSensitiveSampleTypeProcessor("", "", collectionExercise);
    jobTypeProcessor.setSampleOrSensitiveValidationsMap(Map.of("test column", columnValidators));

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("fieldToUpdate", "test column", "newValue", ""));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_OK);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions()).isEmpty();

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(0);
  }

  @Test
  void processChunkBulkUpdateSampleSensitiveUnknownColumn() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.BULK_UPDATE_SAMPLE_SENSITIVE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator[] columnValidators =
        new ColumnValidator[] {
          new ColumnValidator("newValue", false, new Rule[] {new MandatoryRule()})
        };

    Survey survey = new Survey();
    survey.setSampleValidationRules(columnValidators);
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor =
        new BulkUpdateSensitiveSampleTypeProcessor("", "", collectionExercise);
    jobTypeProcessor.setSampleOrSensitiveValidationsMap(Map.of("test column", columnValidators));

    JobRow jobRow = new JobRow();
    jobRow.setRowData(Map.of("fieldToUpdate", "nonexistent column", "newValue", "test data"));
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_ERROR);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions())
        .isEqualTo("fieldToUpdate column nonexistent column does not exist");

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(1);
  }

  @Test
  void processChunkFailsValidationOnUnexpectedErrorDuringValidation() {
    // Given
    CollectionExercise collectionExercise = new CollectionExercise();
    Job job = new Job();
    job.setJobType(JobType.SAMPLE);
    job.setCollectionExercise(collectionExercise);

    ColumnValidator columnValidatorMock = Mockito.mock(ColumnValidator.class);

    Survey survey = new Survey();
    survey.setSampleValidationRules(new ColumnValidator[] {columnValidatorMock});
    collectionExercise.setSurvey(survey);

    JobTypeProcessor jobTypeProcessor = new SampleLoadTypeProcessor("", "", collectionExercise);

    Map<String, String> jobRowData = Map.of("test column", "test@example.com");

    JobRow jobRow = new JobRow();
    jobRow.setRowData(jobRowData);
    List<JobRow> jobRows = List.of(jobRow);

    when(jobTypeHelper.getJobTypeProcessor(job.getJobType(), collectionExercise))
        .thenReturn(jobTypeProcessor);

    when(jobRowRepository.findTop500ByJobAndJobRowStatus(job, JobRowStatus.STAGED))
        .thenReturn(jobRows);

    when(columnValidatorMock.validateRow(jobRowData))
        .thenThrow(new NullPointerException("An unexpected error occured"));

    // When
    underTest.processChunk(job);

    // Then
    ArgumentCaptor<List<JobRow>> jobRowArgumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(jobRowRepository).saveAll(jobRowArgumentCaptor.capture());
    List<JobRow> actualJobRows = jobRowArgumentCaptor.getValue();

    assertThat(actualJobRows.size()).isEqualTo(1);
    assertThat(actualJobRows.get(0).getJobRowStatus()).isEqualTo(JobRowStatus.VALIDATED_ERROR);
    assertThat(actualJobRows.get(0).getValidationErrorDescriptions())
        .isEqualTo(
            "Unexpected technical failure, please report this to the dev team: An unexpected error occured");
    assertThrows(NullPointerException.class, () -> columnValidatorMock.validateRow(jobRowData));

    ArgumentCaptor<Job> jobArgumentCaptor = ArgumentCaptor.forClass(Job.class);
    verify(jobRepository).saveAndFlush(jobArgumentCaptor.capture());
    Job actualJob = jobArgumentCaptor.getValue();

    assertThat(actualJob.getValidatingRowNumber()).isEqualTo(1);
    assertThat(actualJob.getErrorRowCount()).isEqualTo(1);
  }
}
