package protocol.apiversions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static support.KafkaTestFrames.API_VERSIONS_KEY;
import static support.KafkaTestFrames.ApiVersionsReply;
import static support.KafkaTestFrames.apiVersions;
import static support.KafkaTestFrames.parseReply;
import static support.KafkaTestFrames.payload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ApiVersionsResponseEncoderTest {

  private final ApiVersionsRequestDecoder decoder = new ApiVersionsRequestDecoder();
  private final ApiVersionsResponseEncoder encoder = new ApiVersionsResponseEncoder();

  @ParameterizedTest
  @MethodSource("responses")
  void encodesFramedResponseWithMatchingCorrelationAndVersionError(short version,
      int correlationId, short expectedError) throws IOException {
    ApiVersionsRequest request = decoder.decode(
        payload(apiVersions(version, correlationId, "response-test", "tool", "3.5")));

    byte[] frame = encoder.encodeResponse(request).toByte();
    int declaredSize = ByteBuffer.wrap(frame).getInt();
    ApiVersionsReply reply = parseReply(Arrays.copyOfRange(frame, 4, frame.length));

    assertEquals(frame.length - 4, declaredSize);
    assertEquals(correlationId, reply.correlationId());
    assertEquals(expectedError, reply.errorCode());
    assertEquals(1, reply.count());
    assertEquals(API_VERSIONS_KEY, reply.apiKey());
    assertEquals(0, reply.minVersion());
    assertEquals(4, reply.maxVersion());
    assertEquals(0, reply.throttleTime());
  }

  private static Stream<Arguments> responses() {
    return Stream.of(
        Arguments.of((short) 0, 0, (short) 0),
        Arguments.of((short) 3, -17, (short) 0),
        Arguments.of((short) 4, 0x67890abc, (short) 0),
        Arguments.of((short) 5, 0x01020304, (short) 35),
        Arguments.of((short) 26442, 0x4f74d28b, (short) 35)
    );
  }
}
