package server;

import static server.Constants.KAFKA_BROKER_PORT;
import static server.Constants.MAX_MESSAGE_SIZE;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import protocol.apiversions.ApiVersionsRequest;
import protocol.apiversions.ApiVersionsRequestDecoder;
import protocol.apiversions.ApiVersionsResponseEncoder;

public class KafkaServer {

  private volatile boolean serverActive = true;
  private volatile ServerSocket listeningSocket;

  public void startServer() {
    try (ServerSocket serverSocket = new ServerSocket(KAFKA_BROKER_PORT)) {
      serverSocket.setReuseAddress(true);
      listeningSocket = serverSocket;

      while (serverActive) {
        readFromSocket(serverSocket);
      }
    } catch (IOException | NullPointerException e) {
      System.err.println("Exception: " + e.getMessage());
    }
  }

  public void stopServer() {
    serverActive = false;
    ServerSocket socket = listeningSocket;
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException e) {
        System.err.println("Unable to close Socket " + e.getMessage());
      }
    }
  }

  private void readFromSocket(ServerSocket serverSocket) throws IOException {
    Socket client = serverSocket.accept();
    client.setKeepAlive(true);
    try (client; DataInputStream inputStream = new DataInputStream(client.getInputStream())) {
      while (serverActive) {
        handleRequest(client, inputStream);
      }
    } catch (EOFException eofException) {
      System.err.println("Client Disconnected " + eofException.getMessage());
    }
  }

  private void handleRequest(Socket client, DataInputStream inputStream) throws IOException {
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

  }
}
