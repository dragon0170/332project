package cs332.distributedsorting.slave

import cs332.distributedsorting.common.Util.getMyIpAddress

import java.util.concurrent.TimeUnit
import org.apache.logging.log4j.scala.Logging
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import cs332.distributedsorting.sorting.{HandshakeRequest, SortingGrpc}
import cs332.distributedsorting.sorting.SortingGrpc.SortingBlockingStub

object Slave {
  def apply(host: String, port: Int): Slave = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    val blockingStub = SortingGrpc.blockingStub(channel)
    new Slave(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val masterEndpoint = args.headOption
    if (masterEndpoint.isEmpty)
      System.out.println("Master ip:port argument is empty.")
    else {
      val splitedEndpoint = masterEndpoint.get.split(':')
      val client = Slave(splitedEndpoint(0), splitedEndpoint(1).toInt)
      try {
        client.handshake()
      } finally {
        client.shutdown()
      }
    }
  }
}

class Slave private(
  private val channel: ManagedChannel,
  private val blockingStub: SortingBlockingStub
) extends Logging {

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def handshake(): Unit = {
    val request = HandshakeRequest(ipAddress = getMyIpAddress)
    try {
      val response = blockingStub.handshake(request)
      logger.info("Handshake: " + response.ok)
    }
    catch {
      case e: StatusRuntimeException =>
        logger.warn(s"RPC failed: ${e.getStatus}")
    }
  }
}

