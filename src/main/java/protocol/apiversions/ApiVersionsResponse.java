package protocol.apiversions;

import protocol.ResponseHeader;

public record ApiVersionsResponse(
    ResponseHeader header,
    ApiVersionsResponseBody body
) {
  public byte[] toByte() {
    return new ApiVersionsResponseEncoder().toByte(this);
  }
}
