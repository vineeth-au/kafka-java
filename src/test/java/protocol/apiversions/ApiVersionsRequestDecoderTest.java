package protocol.apiversions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static support.KafkaTestFrames.API_VERSIONS_KEY;
import static support.KafkaTestFrames.apiVersions;
import static support.KafkaTestFrames.payload;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ApiVersionsRequestDecoderTest {

  private final ApiVersionsRequestDecoder decoder = new ApiVersionsRequestDecoder();

  @ParameterizedTest
  @MethodSource("headers")
  void decodesHeaderWithVariedCorrelationIdsVersionsAndClientIds(short version, int correlationId,
      String clientId, String softwareName, String softwareVersion) {
    byte[] request = apiVersions(version, correlationId, clientId, softwareName, softwareVersion);

    ApiVersionsRequest decoded = decoder.decode(payload(request));

    assertEquals(API_VERSIONS_KEY, decoded.header().apiKey());
    assertEquals(version, decoded.header().apiVersion());
    assertEquals(correlationId, decoded.header().correlationId());
    assertEquals(clientId, decoded.header().clientId());
    assertNotNull(decoded.body());
  }

  private static Stream<Arguments> headers() {
    return Stream.of(
        Arguments.of((short) 0, 0, "", "legacy", "1"),
        Arguments.of((short) 2, -1, "producer-A", "legacy", "2"),
        Arguments.of((short) 3, 0x67890abc, "kafka-cli", "terminal", "0.1"),
        Arguments.of((short) 4, Integer.MIN_VALUE, "équipe-α", "client-β", "2.7.3"),
        Arguments.of((short) 4, Integer.MAX_VALUE, "consumer-42", "another-client", "10.0"),
        Arguments.of((short) 26442, 0x4f74d28b, "kafka-cli", "kafka-cli", "0.1")
    );
  }
}
