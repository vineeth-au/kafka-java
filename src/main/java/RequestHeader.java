import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@ToString
public final class RequestHeader {

  private final int messageSize;
  private final short apiKey;
  private final short apiVersion;
  private final int correlationId;
  private final String clientId;

  public RequestHeader(byte[] byteArray) {
    messageSize = byteArray.length;
    ByteBuffer byteBuffer = ByteBuffer.wrap(byteArray);

    apiKey = byteBuffer.getShort();
    apiVersion = byteBuffer.getShort();
    correlationId = byteBuffer.getInt();
    clientId = getClientId(byteBuffer);
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