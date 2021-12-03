package uk.gov.ons.ssdc.jobprocessor.model.dto.messaging;

import lombok.Data;

@Data
public class EventDTO {
  private EventHeaderDTO header;
  private PayloadDTO payload;
}
