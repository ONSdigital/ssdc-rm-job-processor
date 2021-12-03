package uk.gov.ons.ssdc.jobprocessor.validators;

import java.util.Optional;
import java.util.UUID;
import org.springframework.context.ApplicationContext;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.config.ApplicationContextProvider;
import uk.gov.ons.ssdc.jobprocessor.repository.CaseRepository;

public class CaseExistsInCollectionExerciseRule implements Rule {
  private final CollectionExercise collectionExercise;
  private static CaseRepository caseRepository = null;

  public CaseExistsInCollectionExerciseRule(CollectionExercise collectionExercise) {
    this.collectionExercise = collectionExercise;
  }

  @Override
  public Optional<String> checkValidity(String data) {

    UUID caseId;
    try {
      caseId = UUID.fromString(data);
    } catch (IllegalArgumentException e) {
      return Optional.of(String.format("Case Id \"%s\" is not a valid UUID format", data));
    }

    if (!getCaseRepository().existsByIdAndCollectionExercise(caseId, collectionExercise)) {
      return Optional.of(
          String.format(
              "Case Id %s does not exist in collection exercise %s",
              caseId, collectionExercise.getName()));
    }

    return Optional.empty();
  }

  private CaseRepository getCaseRepository() {
    if (caseRepository == null) {
      ApplicationContext applicationContext = ApplicationContextProvider.getApplicationContext();
      caseRepository = applicationContext.getBean(CaseRepository.class);
    }

    return caseRepository;
  }
}
