package protocol.apiversions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import protocol.RequestHeader;

public final class ApiVersionsRequestDecoder {

  public ApiVersionsRequest decode(byte[] requestBytes) {
    ByteBuffer buffer = ByteBuffer.wrap(requestBytes);
    RequestHeader header = decodeHeader(buffer);
    ApiVersionsRequestBody body = decodeBody(buffer, header.apiVersion());

    return new ApiVersionsRequest(header, body);
  }

  private RequestHeader decodeHeader(ByteBuffer buffer) {
    short apiKey = buffer.getShort();
    short apiVersion = buffer.getShort();
    int correlationId = buffer.getInt();
    String clientId = getClientId(buffer);

    return new RequestHeader(apiKey, apiVersion, correlationId, clientId);
  }

  private ApiVersionsRequestBody decodeBody(ByteBuffer buffer, short apiVersion) {
    if (apiVersion < 3) {
      return new ApiVersionsRequestBody(null, null);
    }
    // TODO: Hard-coded values needs to be addressed later
    return new ApiVersionsRequestBody("Kafka-cli", Short.toString(apiVersion));
  }

  private String getClientId(ByteBuffer byteBuffer) {
    short clientIdLength = byteBuffer.getShort();
    if (clientIdLength < 0) {
      throw new NullPointerException("Client ID is Null");
    }
    byte[] bytes = new byte[clientIdLength];
    byteBuffer.get(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
