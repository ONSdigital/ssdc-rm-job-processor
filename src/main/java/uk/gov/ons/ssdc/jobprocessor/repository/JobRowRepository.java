package uk.gov.ons.ssdc.jobprocessor.repository;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import uk.gov.ons.ssdc.common.model.entity.Job;
import uk.gov.ons.ssdc.common.model.entity.JobRow;
import uk.gov.ons.ssdc.common.model.entity.JobRowStatus;
import uk.gov.ons.ssdc.common.model.entity.JobStatus;

public interface JobRowRepository extends JpaRepository<JobRow, UUID> {
  boolean existsByJobAndJobRowStatus(Job job, JobRowStatus jobRowStatus);

  // This is required because otherwise Hibernate will attempt to read ALL the JobRows, which
  // could number in the millions, causing an out of memory crash
  @Modifying
  @Query("delete from JobRow r where r.job = :job and r.jobRowStatus = :rowStatus")
  void deleteByJobAndJobRowStatus(
      @Param("job") Job job, @Param("rowStatus") JobRowStatus rowStatus);

  // This is required because otherwise Hibernate will attempt to read ALL the JobRows, which
  // could number in the millions, causing an out of memory crash
  @Modifying
  @Query("delete from JobRow r where r.job = :job")
  void deleteByJob(@Param("job") Job job);

  List<JobRow> findTop500ByJobAndJobRowStatus(Job job, JobRowStatus jobRowStatus);

  List<JobRow> findTop500ByJob_JobStatus(JobStatus jobStatus);
}
