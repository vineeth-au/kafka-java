package protocol.apiversions;

import protocol.RequestHeader;

public record ApiVersionsRequest(
    RequestHeader header,
    ApiVersionsRequestBody body
) {

}
