package uk.gov.ons.ssdc.jobprocessor.repository;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import uk.gov.ons.ssdc.common.model.entity.Case;
import uk.gov.ons.ssdc.common.model.entity.CollectionExercise;

public interface CaseRepository extends JpaRepository<Case, UUID> {
  boolean existsByIdAndCollectionExercise(UUID caseId, CollectionExercise collectionExercise);

  @Query(
      value =
          "SELECT exists(select 1 FROM casev3.cases WHERE sample->>'PARTICIPANT_ID' = :participantId and collection_exercise_id = :collectionExerciseId)",
      nativeQuery = true)
  boolean existsByCaseByParticipantIdAndCollectionExercise(
      @Param("participantId") String participantId,
      @Param("collectionExerciseId") UUID collectionExerciseId);

  @Query(
      value =
          "SELECT * FROM casev3.cases WHERE sample->>'PARTICIPANT_ID' = :participantId and collection_exercise_id = :collectionExerciseId",
      nativeQuery = true)
  Case findCaseByParticipantIdAndCollectionExercise(
      @Param("participantId") String participantId,
      @Param("collectionExerciseId") UUID collectionExerciseId);
}
