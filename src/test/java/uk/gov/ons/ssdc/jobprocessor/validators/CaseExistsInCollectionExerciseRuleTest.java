package uk.gov.ons.ssdc.jobprocessor.validators;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.jobprocessor.repository.CaseRepository;

@ExtendWith(MockitoExtension.class)
class CaseExistsInCollectionExerciseRuleTest {

  @Mock CaseRepository caseRepository;

  @Test
  void checkValidityValid() {
    // Given
    UUID caseId = UUID.randomUUID();
    CollectionExercise collectionExercise = new CollectionExercise();
    collectionExercise.setId(UUID.randomUUID());
    CaseExistsInCollectionExerciseRule caseExistsInCollectionExerciseRule =
        new CaseExistsInCollectionExerciseRule(collectionExercise);
    ReflectionTestUtils.setField(
        caseExistsInCollectionExerciseRule, "caseRepository", caseRepository);
    when(caseRepository.existsByIdAndCollectionExercise(caseId, collectionExercise))
        .thenReturn(true);

    // When
    Optional<String> validitionFailure =
        caseExistsInCollectionExerciseRule.checkValidity(caseId.toString());

    // Then
    assertThat(validitionFailure).isNotPresent();
  }

  @Test
  void checkValidityNotValidCaseDoesNotExist() {
    // Given
    UUID caseId = UUID.randomUUID();
    CollectionExercise collectionExercise = new CollectionExercise();
    collectionExercise.setId(UUID.randomUUID());
    CaseExistsInCollectionExerciseRule caseExistsInCollectionExerciseRule =
        new CaseExistsInCollectionExerciseRule(collectionExercise);
    ReflectionTestUtils.setField(
        caseExistsInCollectionExerciseRule, "caseRepository", caseRepository);
    when(caseRepository.existsByIdAndCollectionExercise(caseId, collectionExercise))
        .thenReturn(false);

    // When
    Optional<String> validationFailure =
        caseExistsInCollectionExerciseRule.checkValidity(caseId.toString());

    // Then
    assertThat(validationFailure)
        .contains(
            String.format(
                "Case Id %s does not exist in collection exercise %s",
                caseId, collectionExercise.getName()));
  }

  @Test
  void checkValidityNotValidBadFormat() {
    // Given
    String invalidFormatCaseId = "Not a valid UUID";
    CollectionExercise collectionExercise = new CollectionExercise();
    collectionExercise.setId(UUID.randomUUID());
    CaseExistsInCollectionExerciseRule caseExistsInCollectionExerciseRule =
        new CaseExistsInCollectionExerciseRule(collectionExercise);

    // When
    Optional<String> validationFailure =
        caseExistsInCollectionExerciseRule.checkValidity(invalidFormatCaseId);

    // Then
    assertThat(validationFailure)
        .contains(String.format("Case Id \"%s\" is not a valid UUID format", invalidFormatCaseId));
  }
}
