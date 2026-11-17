import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {

  private static final int PORT = 9092;
  private static final short MAX_SUPPORTED_VERSION = 4;
  private static final int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

  static void main(String[] arg) {
    Main main = new Main();
    main.startServer();
  }

  private void startServer() {
    try (ServerSocket serverSocket = new ServerSocket(PORT)) {
      serverSocket.setReuseAddress(true);
      try (Socket client = serverSocket.accept()) {
        handleRequest(client);
      }
    } catch (IOException e) {
      System.err.println("IOException: " + e.getMessage());
    } catch (NullPointerException nullPointer) {
      System.err.println("Value is Null " + nullPointer.getMessage());
    }
  }

  private void handleRequest(Socket client) throws IOException {
    DataInputStream inputStream = new DataInputStream(client.getInputStream());
    int messageSize = inputStream.readInt();

    if (messageSize < 0 || messageSize > MAX_MESSAGE_SIZE) {
      throw new IOException("Invalid Message Size " + messageSize);
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
    System.out.println(requestHeader);

    short errorCode =
        requestHeader.getApiVersion() > MAX_SUPPORTED_VERSION ? (short) 35 : (short) 0;

    return ByteBuffer.allocate(requestHeader.getMessageSize())
        .putInt(6) // message_size: bytes after this field
        .putInt(requestHeader.getCorrelationId())
        .putShort(errorCode)
        .array();
  }
}
