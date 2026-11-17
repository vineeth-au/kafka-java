import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

public class Main {

  public static final int PORT = 9092;

  static void main(String[] arg) {
    System.err.println("Logs from your program will appear here!");
    try (ServerSocket serverSocket = new ServerSocket(PORT)) {
      serverSocket.setReuseAddress(true);
      try (Socket client = serverSocket.accept()) {
        DataInputStream inputStream = new DataInputStream(client.getInputStream());
        int messageSize = inputStream.readInt();

        if (messageSize < 0) {
          throw new IOException("Message Size is 0");
        }
        byte[] messageInBytes = new byte[messageSize];
        inputStream.readFully(messageInBytes);
        RequestHeader requestHeader = new RequestHeader(messageInBytes);

        byte[] response = ByteBuffer.allocate(8)
            .putInt(4)
            .putInt(requestHeader.getCorrelationId())
            .array();

        OutputStream outputStream = client.getOutputStream();
        outputStream.write(response);
        outputStream.close();
      }
    } catch (IOException e) {
      System.err.println("IOException: " + e.getMessage());
    }
  }
}
