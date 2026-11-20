package protocol.apiversions;

public record ApiVersionsResponseBody(
    short errorCode,
    ApiVersionsResponseArray apiVersionsResponseArray,
    int throttleTime,
    byte tagBuffer
) {

}
