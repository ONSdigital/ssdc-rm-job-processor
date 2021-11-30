package uk.gov.ons.ssdc.jobprocessor.utility;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import uk.gov.ons.ssdc.common.model.entity.Case;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;

public interface CaseRepository extends JpaRepository<Case, UUID> {
  boolean existsByIdAndCollectionExercise(UUID caseId, CollectionExercise collectionExercise);
}
