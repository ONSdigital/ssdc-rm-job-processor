package uk.gov.ons.ssdc.jobprocessor.repository;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Component;
import org.springframework.test.context.ActiveProfiles;
import uk.gov.ons.ssdc.common.model.entity.Survey;

@Component
@ActiveProfiles("test")
public interface SurveyRepository extends JpaRepository<Survey, UUID> {}
