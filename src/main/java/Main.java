import server.KafkaServer;

void main() {
  KafkaServer kafkaServer = new KafkaServer();
  Runtime.getRuntime().addShutdownHook(new Thread(kafkaServer::stopServer));

  kafkaServer.startServer();
}
