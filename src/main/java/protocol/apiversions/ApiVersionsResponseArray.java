package protocol.apiversions;

import java.util.List;

public record ApiVersionsResponseArray(
    byte arrayLength,
    List<ApiVersion> apiVersions) {
  public ApiVersionsResponseArray {
    arrayLength = (byte) (apiVersions.size() + 1);
  }
}
