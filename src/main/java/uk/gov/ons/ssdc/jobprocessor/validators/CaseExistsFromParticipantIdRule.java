package uk.gov.ons.ssdc.jobprocessor.validators;

import java.util.Optional;
import org.springframework.context.ApplicationContext;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.config.ApplicationContextProvider;
import uk.gov.ons.ssdc.jobprocessor.repository.CaseRepository;

public class CaseExistsFromParticipantIdRule implements Rule {
  private final CollectionExercise collectionExercise;
  private static CaseRepository caseRepository = null;

  public CaseExistsFromParticipantIdRule(CollectionExercise collectionExercise) {
    this.collectionExercise = collectionExercise;
  }

  @Override
  public Optional<String> checkValidity(String data) {

    if (!getCaseRepository()
        .existsByCaseByParticipantIdAndCollectionExercise(data, collectionExercise.getId())) {
      return Optional.of(String.format("Participant Id %s does not exist for case", data));
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
