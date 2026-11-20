package server;

import static server.Constants.KAFKA_BROKER_PORT;
import static server.Constants.MAX_MESSAGE_SIZE;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import protocol.apiversions.ApiVersionsRequest;
import protocol.apiversions.ApiVersionsRequestDecoder;
import protocol.apiversions.ApiVersionsResponseEncoder;

public class KafkaServer {

  public void startServer() {
    try (ServerSocket serverSocket = new ServerSocket(KAFKA_BROKER_PORT)) {
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

  private void handleRequest(Socket client) {
    try (DataInputStream inputStream = new DataInputStream(client.getInputStream())) {
      int messageSize = inputStream.readInt();
      if (messageSize < 0 || messageSize > MAX_MESSAGE_SIZE) {
        throw new IOException("Invalid Message Size " + messageSize);
      }
      byte[] requestBytes = new byte[messageSize];
      inputStream.readFully(requestBytes);

      ApiVersionsRequest decodedRequest = new ApiVersionsRequestDecoder().decode(requestBytes);
      byte[] apiVersionsResponse = new ApiVersionsResponseEncoder()
          .encodeResponse(decodedRequest)
          .toByte();

      OutputStream outputStream = client.getOutputStream();
      outputStream.write(apiVersionsResponse);
      outputStream.flush();
    } catch (IOException ioException) {
      System.err.println("IOException: " + ioException.getMessage());
    }
  }
}
