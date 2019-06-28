package org.gregory.utils;

import java.io.Serializable;
import java.util.Arrays;

public class FailureObject implements Serializable {
  private String failedClass;
  private String errorMessage;
  private String stackTrace;

  public FailureObject(Object failedParsing, Throwable wasThrown) {
    this.failedClass = failedParsing.getClass().toString();
    this.stackTrace = Arrays.toString(wasThrown.getStackTrace());
    this.errorMessage = wasThrown.getLocalizedMessage();
  }

  @Override
  public String toString() {
    return String.format("%s failed: %s\n%s", this.failedClass, this.errorMessage, this.stackTrace);
  }
}
