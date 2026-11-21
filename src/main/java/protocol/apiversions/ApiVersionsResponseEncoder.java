package protocol.apiversions;

import static util.Constants.MAX_SUPPORTED_VERSION;
import static util.Constants.NO_ERROR;
import static util.Constants.UNSUPPORTED_VERSION;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import protocol.RequestHeader;
import protocol.ResponseHeader;

public final class ApiVersionsResponseEncoder {

  public ApiVersionsResponse encodeResponse(ApiVersionsRequest request) {

    RequestHeader header = request.header();

    short errorCode = header
        .apiVersion() > MAX_SUPPORTED_VERSION ? UNSUPPORTED_VERSION : NO_ERROR;
    List<ApiVersion> apiVersion = List.of(
        new ApiVersion(header.apiKey(), (short) 0, (short) 4, (byte) 0));
    byte length = (byte) (apiVersion.size() + 1);

    ApiVersionsResponseArray apiVersionsResponseArray = new ApiVersionsResponseArray(length,
        apiVersion);
    ApiVersionsResponseBody apiVersionsResponseBody = new ApiVersionsResponseBody(errorCode,
        apiVersionsResponseArray, 0, (byte) 0);

    return new ApiVersionsResponse(
        new ResponseHeader(header.correlationId()),
        apiVersionsResponseBody
    );
  }

  public byte[] toByte(ApiVersionsResponse response) {
    ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
    try (DataOutputStream payload = new DataOutputStream(payloadBytes)) {
      payload.writeInt(response.header().correlationId());
      payload.writeShort(response.body().errorCode());

      List<ApiVersion> apiVersions = response.body().apiVersionsResponseArray().apiVersions();
      writeUnsignedVarInt(payload, apiVersions.size() + 1);
      for (ApiVersion version : apiVersions) {
        payload.writeShort(version.apiKey());
        payload.writeShort(version.minSupportedApiVersion());
        payload.writeShort(version.maxSupportedApiVersion());
        payload.writeByte(version.tagBuffer());
      }
      payload.writeInt(response.body().throttleTime());
      payload.writeByte(response.body().tagBuffer());

      ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
      DataOutputStream frame = new DataOutputStream(frameBytes);
      byte[] encodedPayload = payloadBytes.toByteArray();
      frame.writeInt(encodedPayload.length);
      frame.write(encodedPayload);

      return frameBytes.toByteArray();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void writeUnsignedVarInt(DataOutputStream out, int value) throws IOException {
    if (value < 0) {
      throw new IllegalStateException("Value must not be negative");
    }
    while (value >= 128) {
      int currentByte = (value % 128) + 128;
      out.writeByte(currentByte);
      value /= 128;
    }
    out.writeByte(value);
  }
}
