package server;


import static util.Constants.EMPTY_MESSAGE;
import static util.Constants.MAX_MESSAGE_SIZE;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import protocol.apiversions.ApiVersionsRequest;
import protocol.apiversions.ApiVersionsRequestDecoder;
import protocol.apiversions.ApiVersionsResponseEncoder;

final class ClientHandler implements Runnable {

  private final Socket client;

  ClientHandler(final Socket client) {
    this.client = client;
  }

  @Override
  public void run() {
    try {
      readFromSocket();
    } catch (Exception e) {
      throw new RuntimeException("ClientHandler failure", e);
    }
  }

  void readFromSocket() throws IOException {
    client.setKeepAlive(true);
    try (client; DataInputStream inputStream = new DataInputStream(client.getInputStream())) {
      while(!client.isClosed()) {
        handleRequests(client, inputStream);
      }
    } catch (EOFException e) {
      // TODO: This can be a DEBUG Level statement
      // System.err.println("Client Disconnected");
    }
  }

  private void handleRequests(Socket client, DataInputStream inputStream) throws IOException {
    int messageSize = inputStream.readInt();
    if (messageSize < EMPTY_MESSAGE || messageSize > MAX_MESSAGE_SIZE) {
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

  void stop() {
    try {
      client.get
      client.close();
    } catch (IOException e) {
      System.out.println("ClientHandler Stopper");
    }
  }
}
