package support;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;

public final class KafkaTestFrames {

  public static final short API_VERSIONS_KEY = 18;
  public static final String SUPPORTED_SAMPLE =
      "0000001a0012000467890abc00096b61666b612d636c69000a6b61666b612d636c6904302e3100";
  public static final String UNSUPPORTED_SAMPLE =
      "000000230012674a4f74d28b00096b61666b612d636c69000a6b61666b612d636c6904302e3100";

  private KafkaTestFrames() {
  }

  public static byte[] hex(String value) {
    return HexFormat.of().parseHex(value);
  }

  public static byte[] apiVersions(short version, int correlationId, String clientId,
      String softwareName, String softwareVersion) {
    try {
      ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
      DataOutputStream payload = new DataOutputStream(payloadBytes);
      payload.writeShort(API_VERSIONS_KEY);
      payload.writeShort(version);
      payload.writeInt(correlationId);
      if (clientId == null) {
        payload.writeShort(-1);
      } else {
        byte[] id = clientId.getBytes(StandardCharsets.UTF_8);
        payload.writeShort(id.length);
        payload.write(id);
      }
      if (version >= 3) {
        payload.writeByte(0);
        writeCompactString(payload, softwareName);
        writeCompactString(payload, softwareVersion);
        payload.writeByte(0);
      }
      byte[] bytes = payloadBytes.toByteArray();
      ByteArrayOutputStream framedBytes = new ByteArrayOutputStream();
      DataOutputStream frame = new DataOutputStream(framedBytes);
      frame.writeInt(bytes.length);
      frame.write(bytes);
      return framedBytes.toByteArray();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  private static void writeCompactString(DataOutputStream output, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length >= 127) {
      throw new IllegalArgumentException("Test fixture string is too long");
    }
    output.writeByte(bytes.length + 1);
    output.write(bytes);
  }

  public static byte[] payload(byte[] frame) {
    int size = ByteBuffer.wrap(frame).getInt();
    if (size != frame.length - 4) {
      throw new IllegalArgumentException("Request size does not match its payload");
    }
    return Arrays.copyOfRange(frame, 4, frame.length);
  }

  public static ApiVersionsReply readReply(Socket socket) throws IOException {
    DataInputStream input = new DataInputStream(socket.getInputStream());
    int size = input.readInt();
    if (size < 0 || size > 1024) {
      throw new IOException("Unexpected response size: " + size);
    }
    byte[] payload = new byte[size];
    input.readFully(payload);
    return parseReply(payload);
  }

  public static ApiVersionsReply parseReply(byte[] payload) throws IOException {
    DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload));
    int correlationId = input.readInt();
    short errorCode = input.readShort();
    int count = input.readUnsignedByte() - 1; // Compact array length for current responses
    if (count < 0 || count > 10) {
      throw new IOException("Unexpected number of API versions: " + count);
    }
    short apiKey = -1;
    short minVersion = -1;
    short maxVersion = -1;
    for (int index = 0; index < count; index++) {
      apiKey = input.readShort();
      minVersion = input.readShort();
      maxVersion = input.readShort();
      if (input.readUnsignedByte() != 0) {
        throw new IOException("Unexpected API version tagged fields");
      }
    }
    int throttleTime = input.readInt();
    if (input.readUnsignedByte() != 0 || input.available() != 0) {
      throw new IOException("Unexpected response trailing bytes");
    }
    return new ApiVersionsReply(correlationId, errorCode, count, apiKey, minVersion, maxVersion,
        throttleTime);
  }

  public record ApiVersionsReply(int correlationId, short errorCode, int count, short apiKey,
                                 short minVersion, short maxVersion, int throttleTime) {
  }
}
