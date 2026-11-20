package protocol;

public record RequestHeader(
    short apiKey,
    short apiVersion,
    int correlationId,
    String clientId
) {

}