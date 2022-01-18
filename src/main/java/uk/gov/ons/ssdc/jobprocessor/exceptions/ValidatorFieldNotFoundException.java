package uk.gov.ons.ssdc.jobprocessor.exceptions;

public class ValidatorFieldNotFoundException extends Exception {
  public ValidatorFieldNotFoundException(String errorMessage) {
    super(errorMessage);
  }
}
