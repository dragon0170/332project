package cs332.distributedsorting.slave

import java.util.concurrent.TimeUnit
import java.net._
import java.util.logging.{Level, Logger}
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}

import cs332.distributedsorting.example.{GreeterGrpc, HelloIpAdress}
import cs332.distributedsorting.example.GreeterGrpc.GreeterBlockingStub

object Slave {
  def apply(host: String, port: Int): Slave = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    val blockingStub = GreeterGrpc.blockingStub(channel)
    new Slave(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val client = Slave("localhost", 50061)
    try {
      val user = args.headOption.getOrElse("world")
      val localhost: InetAddress = InetAddress.getLocalHost
      val localIpAddress: String = localhost.getHostAddress
      client.sendMyIp(user, localIpAddress)
    } finally {
      client.shutdown()
    }
  }
}

class Slave private(
  private val channel: ManagedChannel,
  private val blockingStub: GreeterBlockingStub
) {
  private[this] val logger = Logger.getLogger(classOf[Slave].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def sendMyIp(name: String, ip : String): Unit = {
    logger.info("Will try to greet " + name + " ...")
    val request = HelloIpAdress(ipadress = ip, name = name)
    try {
      val response = blockingStub.makeContact(request)
      logger.info("Greeting: " + response.message)
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
    }
  }
}

