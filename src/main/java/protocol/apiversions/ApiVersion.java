package protocol.apiversions;

public record ApiVersion(
    short apiKey,
    short minSupportedApiVersion,
    short maxSupportedApiVersion,
    byte tagBuffer
) {

}
