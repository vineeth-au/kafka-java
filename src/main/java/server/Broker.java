package server;

import static util.Constants.KAFKA_BROKER_PORT;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public final class Broker {

  private volatile boolean serverActive = true;
  private volatile ServerSocket listeningSocket;
  private final List<ClientHandler> clientListener = new ArrayList<>();

  public void start() {
    try (ServerSocket serverSocket = new ServerSocket(KAFKA_BROKER_PORT)) {
      serverSocket.setReuseAddress(true);
      listeningSocket = serverSocket;

      while (serverActive) {
        Socket clientSocket = serverSocket.accept();
        ClientHandler clientHandler = new ClientHandler(clientSocket);
        clientListener.add(clientHandler);
        new Thread(clientHandler).start();
      }

    } catch (IOException | RuntimeException e) {
      if (serverActive) {
        System.err.println("Broker failure: " + e);
      }
    }
  }

  public void stop() {
    serverActive = false;
    ServerSocket socket = listeningSocket;
    if (socket != null) {
      try {
        socket.close();
        clientListener.forEach(ClientHandler::stop);
      } catch (IOException e) {
        System.err.println("Unable to close Socket " + e.getMessage());
      }
    }
  }
}
