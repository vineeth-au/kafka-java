package protocol.apiversions;

public record ApiVersionsRequestBody(
    String clientSoftwareName,
    String clientSoftwareVersion
) {

}
