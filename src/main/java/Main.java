import server.Broker;

void main() {
  Broker broker = new Broker();
  Runtime.getRuntime().addShutdownHook(new Thread(broker::stop));

  broker.start();
}
