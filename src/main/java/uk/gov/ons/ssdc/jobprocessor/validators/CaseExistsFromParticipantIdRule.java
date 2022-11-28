package uk.gov.ons.ssdc.jobprocessor.validators;

import org.springframework.context.ApplicationContext;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;
import uk.gov.ons.ssdc.common.validation.Rule;
import uk.gov.ons.ssdc.jobprocessor.config.ApplicationContextProvider;
import uk.gov.ons.ssdc.jobprocessor.repository.CaseRepository;

import java.util.Optional;

public class CaseExistsFromParticipantIdRule implements Rule {
  private static CaseRepository caseRepository = null;

  public CaseExistsFromParticipantIdRule() {
  }

  @Override
  public Optional<String> checkValidity(String data) {

    if (!getCaseRepository().existsByCaseByParticipantId(data)) {
      return Optional.of(
          String.format(
              "Participant Id %s does not exist for case",
              data));
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
