package cs332.distributedsorting.master

import org.apache.logging.log4j.scala.Logging
import io.grpc.{Server, ServerBuilder}
import cs332.distributedsorting.sorting.{HandshakeRequest, HandshakeResponse, SortingGrpc}
import cs332.distributedsorting.common.Util.getMyIpAddress

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}

object Master {
  def main(args: Array[String]): Unit = {
    val numClient = args.headOption
    if (numClient.isEmpty) return

    val server = new Master(ExecutionContext.global, numClient.get.toInt)
    server.start()
    server.printEndpoint()
    server.blockUntilShutdown()
  }

  private val port = 50051
}

class SlaveClient(val id: Int, val ip: String) {
  override def toString: String = ip
}

class Master(executionContext: ExecutionContext, val numClient: Int) extends Logging { self =>
  private[this] var server: Server = null
  private val clientLatch: CountDownLatch = new CountDownLatch(numClient)
  var slaves: Vector[SlaveClient] = Vector.empty

  def start(): Unit = {
    server = ServerBuilder.forPort(Master.port).addService(SortingGrpc.bindService(new SortingImpl, executionContext)).build.start
    logger.info("Server numClient: " + self.numClient)
    logger.info("Server started, listening on " + Master.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def printEndpoint(): Unit = {
    System.out.println(getMyIpAddress + ":" + Master.port)
  }

  def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private def addNewSlave(ipAddress: String): Unit = {
    this.synchronized {
      this.slaves = this.slaves :+ new SlaveClient(this.slaves.length, ipAddress)
      if (this.slaves.length == this.numClient) printSlaveIpAddresses()
    }
  }

  private def printSlaveIpAddresses(): Unit = {
    System.out.println(this.slaves.mkString(", "))
  }

  private class SortingImpl extends SortingGrpc.Sorting {
    override def handshake(req: HandshakeRequest) = {
      logger.info("Handshake from " + req.ipAddress)
      clientLatch.countDown()
      addNewSlave(req.ipAddress)
      clientLatch.await()
      val reply = HandshakeResponse(ok = true)
      Future.successful(reply)
    }
  }

}
