package server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static support.KafkaTestFrames.API_VERSIONS_KEY;
import static support.KafkaTestFrames.SUPPORTED_SAMPLE;
import static support.KafkaTestFrames.UNSUPPORTED_SAMPLE;
import static support.KafkaTestFrames.ApiVersionsReply;
import static support.KafkaTestFrames.apiVersions;
import static support.KafkaTestFrames.hex;
import static support.KafkaTestFrames.readReply;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ClientHandlerIntegrationTest {

  @Test
  void suppliedSupportedVersionSampleReturnsCorrelationId() throws Exception {
    try (TcpHarness broker = new TcpHarness(); Socket client = broker.connect()) {
      broker.expectMalformedTrailingFrame();
      client.getOutputStream().write(hex(SUPPORTED_SAMPLE));

      assertReply(readReply(client), 0x67890abc, (short) 0);
    }
  }

  @Test
  void suppliedUnsupportedVersionSampleReturnsError() throws Exception {
    try (TcpHarness broker = new TcpHarness(); Socket client = broker.connect()) {
      client.getOutputStream().write(hex(UNSUPPORTED_SAMPLE));

      assertReply(readReply(client), 0x4f74d28b, (short) 35);
    }
  }

  @Test
  void completeVersionFourRequestReturnsResponse() throws Exception {
    try (TcpHarness broker = new TcpHarness(); Socket client = broker.connect()) {
      client.getOutputStream().write(
          apiVersions((short) 4, 0x10203040, "cli-27", "testing-tool", "5.2"));

      assertReply(readReply(client), 0x10203040, (short) 0);
    }
  }

  @Test
  void readsRequestWrittenInFragments() throws Exception {
    try (TcpHarness broker = new TcpHarness(); Socket client = broker.connect()) {
      byte[] frame = apiVersions((short) 4, 73, "fragmented", "client", "1.0");
      for (int offset = 0; offset < frame.length; offset += 3) {
        client.getOutputStream().write(frame, offset, Math.min(3, frame.length - offset));
        client.getOutputStream().flush();
      }

      assertReply(readReply(client), 73, (short) 0);
    }
  }

  @Test
  void handlesSerialRequestsOnOneConnection() throws Exception {
    try (TcpHarness broker = new TcpHarness(); Socket client = broker.connect()) {
      short[] versions = {0, 3, 4, 5, 4};
      int[] ids = {0, -7, 0x11223344, Integer.MAX_VALUE, Integer.MIN_VALUE};
      for (int i = 0; i < ids.length; i++) {
        client.getOutputStream().write(
            apiVersions(versions[i], ids[i], "serial-" + i, "cli-" + i, "2." + i));
        assertReply(readReply(client), ids[i], versions[i] > 4 ? (short) 35 : (short) 0);
      }
    }
  }

  @Test
  void handlesPipelinedRequestsOnOneConnection() throws Exception {
    try (TcpHarness broker = new TcpHarness(); Socket client = broker.connect()) {
      for (int id : new int[] {19, 29, 39}) {
        client.getOutputStream().write(
            apiVersions((short) 4, id, "pipeline", "batch-client", "1.4"));
      }
      client.getOutputStream().flush();

      for (int id : new int[] {19, 29, 39}) {
        assertReply(readReply(client), id, (short) 0);
      }
    }
  }

  @Test
  void handlesConcurrentClientsWithIndependentCorrelationIds() throws Exception {
    try (TcpHarness broker = new TcpHarness();
         ExecutorService workers = Executors.newFixedThreadPool(6)) {
      CountDownLatch start = new CountDownLatch(1);
      List<Future<?>> results = new ArrayList<>();
      for (int clientIndex = 0; clientIndex < 6; clientIndex++) {
        int index = clientIndex;
        results.add(workers.submit(() -> {
          try (Socket client = broker.connect()) {
            if (!start.await(2, TimeUnit.SECONDS)) {
              throw new AssertionError("Clients did not start together");
            }
            for (int request = 0; request < 4; request++) {
              int correlationId = (index + 1) * 1000 + request;
              short version = (short) (request == 3 ? 5 : 4);
              client.getOutputStream().write(apiVersions(version, correlationId,
                  "client-" + index, "load-" + index, "1." + request));
              assertReply(readReply(client), correlationId, version == 5 ? (short) 35 : (short) 0);
            }
          } catch (Exception e) {
            throw new AssertionError(e);
          }
        }));
      }
      start.countDown();
      for (Future<?> result : results) {
        result.get(10, TimeUnit.SECONDS);
      }
    }
  }

  private static void assertReply(ApiVersionsReply reply, int correlationId, short errorCode) {
    assertEquals(correlationId, reply.correlationId());
    assertEquals(errorCode, reply.errorCode());
    assertEquals(1, reply.count());
    assertEquals(API_VERSIONS_KEY, reply.apiKey());
    assertEquals(0, reply.minVersion());
    assertEquals(4, reply.maxVersion());
    assertEquals(0, reply.throttleTime());
  }

  private static final class TcpHarness implements AutoCloseable {

    private final ServerSocket listener;
    private final Thread acceptThread;
    private final List<ClientHandler> handlers = new ArrayList<>();
    private final List<Thread> handlerThreads = new ArrayList<>();
    private final List<Throwable> handlerFailures = new ArrayList<>();
    private boolean malformedTrailingFrameExpected;

    private TcpHarness() throws IOException {
      listener = new ServerSocket(0, 16, InetAddress.getLoopbackAddress());
      acceptThread = new Thread(() -> {
        try {
          while (!listener.isClosed()) {
            ClientHandler handler = new ClientHandler(listener.accept());
            synchronized (handlers) {
              handlers.add(handler);
            }
            Thread thread = new Thread(handler, "test-client-handler");
            thread.setUncaughtExceptionHandler((ignored, error) -> {
              synchronized (handlerFailures) {
                handlerFailures.add(error);
              }
            });
            synchronized (handlerThreads) {
              handlerThreads.add(thread);
            }
            thread.start();
          }
        } catch (SocketException closed) {
          if (!listener.isClosed()) {
            throw new AssertionError(closed);
          }
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      }, "test-accept-loop");
      acceptThread.start();
    }

    private Socket connect() throws IOException {
      Socket socket = new Socket(InetAddress.getLoopbackAddress(), listener.getLocalPort());
      socket.setSoTimeout(2_000);
      return socket;
    }

    private void expectMalformedTrailingFrame() {
      malformedTrailingFrameExpected = true;
    }

    @Override
    public void close() throws Exception {
      listener.close();
      acceptThread.join(2_000);
      synchronized (handlers) {
        handlers.forEach(ClientHandler::stop);
      }
      synchronized (handlerThreads) {
        for (Thread thread : handlerThreads) {
          thread.join(2_000);
          assertFalse(thread.isAlive(), "Handler did not stop");
        }
      }
      synchronized (handlerFailures) {
        if (malformedTrailingFrameExpected) {
          assertEquals(1, handlerFailures.size(), "Expected one invalid trailing frame");
          assertTrue(handlerFailures.getFirst().getCause().getMessage()
              .startsWith("Invalid Message Size"));
        } else {
          assertTrue(handlerFailures.isEmpty(), "Unexpected handler failures: " + handlerFailures);
        }
      }
    }
  }
}
