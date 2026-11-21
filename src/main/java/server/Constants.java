package server;

public final class Constants {

  private Constants() {
    throw new AssertionError("Constants class cannot be instantiated");
  }

  public static final int KAFKA_BROKER_PORT = 9092;
  public static final short MAX_SUPPORTED_VERSION = 4;
  public static final int EMPTY_MESSAGE = 0;
  public static final int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;
  public static final short UNSUPPORTED_VERSION = 35;
  public static final short NO_ERROR = 0;

}
