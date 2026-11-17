import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {

  public static final int PORT = 9092;
  public static final short MAX_SUPPORTED_VERSION = 4;

  static void main(String[] arg) {
    Main main = new Main();
    main.startServer();
  }

  void startServer() {
    System.err.println("Logs from your program will appear here!");
    try (ServerSocket serverSocket = new ServerSocket(PORT)) {
      serverSocket.setReuseAddress(true);
      try (Socket client = serverSocket.accept()) {
        handleRequest(client);
      }
    } catch (IOException e) {
      System.err.println("IOException: " + e.getMessage());
    }
  }

  private void handleRequest(Socket client) throws IOException {
    DataInputStream inputStream = new DataInputStream(client.getInputStream());
    int messageSize = inputStream.readInt();

    if (messageSize < 0) {
      throw new IOException("Message Size is 0");
    }
    byte[] requestBytes = new byte[messageSize];
    inputStream.readFully(requestBytes);
    byte[] response = encodeApiVersionsResponse(requestBytes);

    OutputStream outputStream = client.getOutputStream();
    outputStream.write(response);
    outputStream.close();
  }

  private byte[] encodeApiVersionsResponse(byte[] requestBytes) {
    RequestHeader requestHeader = new RequestHeader(requestBytes);
    ByteBuffer responseBuffer = ByteBuffer.allocate(10)
        .putInt(4) // message
        .putInt(requestHeader.getCorrelationId());

    if (requestHeader.getApiVersion() > MAX_SUPPORTED_VERSION) {
      responseBuffer.putShort((short) 35);
    } else {
      responseBuffer.putShort((short) 0);
    }

    return responseBuffer.array();
  }
}
