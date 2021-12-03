package uk.gov.ons.ssdc.jobprocessor.healthcheck;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class HealthCheck {
  @Value("${healthcheck.filename}")
  private String fileName;

  @Scheduled(fixedDelayString = "${healthcheck.frequency}")
  public void updateFileWithCurrentTimestamp() {
    Path path = Paths.get(fileName);
    OffsetDateTime now = OffsetDateTime.now();

    try (BufferedWriter writer = Files.newBufferedWriter(path)) {
      writer.write(now.toString());
    } catch (IOException e) {
      // Ignored
    }
  }
}
