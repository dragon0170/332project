package cs332.distributedsorting.master

import java.util.logging.Logger
import io.grpc.{Server, ServerBuilder}
import cs332.distributedsorting.sorting.{HandshakeRequest, HandshakeResponse, SortingGrpc,SendDataRequest, SendDataResponse}
import cs332.distributedsorting.common.Util.getMyIpAddress

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}

object Master {
  private val logger = Logger.getLogger(classOf[Master].getName)

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

class Master(executionContext: ExecutionContext, val numClient: Int) { self =>
  private[this] var server: Server = null
  private val clientLatch: CountDownLatch = new CountDownLatch(numClient)
  var slaves: Vector[SlaveClient] = Vector.empty
  var data : List[Array[Byte]] = Nil
  var count = 0

  private def start(): Unit = {
    server = ServerBuilder.forPort(Master.port).addService(SortingGrpc.bindService(new SortingImpl, executionContext)).build.start
    Master.logger.info("Server numClient: " + self.numClient)
    Master.logger.info("Server started, listening on " + Master.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def printEndpoint(): Unit = {
    System.out.println(getMyIpAddress + ":" + Master.port)
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }


  private def addData(data : String,ipAddress : String) : Boolean = {
    this.synchronized({
      if (slaves.filter(x=>x.ip == ipAddress).isEmpty)
        System.out.println("we receive data from a client we did not register")
        return false 
      count +=1
      this.data :+ data.getBytes().grouped(10).toList
      if(count == numClient)
        System.out.println("we receive all the data")
      return true
    })
  }

  private def addNewSlave(ipAddress: String): Unit = {
    this.synchronized {
      this.slaves = this.slaves :+ new SlaveClient(this.slaves.length, ipAddress)
      if (this.slaves.length == this.numClient) {
        printSlaveIpAddresses()
      }
    }
  }

  private def printSlaveIpAddresses(): Unit = {
    System.out.println(this.slaves.mkString(", "))
  }

  private class SortingImpl extends SortingGrpc.Sorting {

    override def handshake(req: HandshakeRequest) = {
      Master.logger.info("Handshake from " + req.ipAddress)
      clientLatch.countDown()
      addNewSlave(req.ipAddress)
      clientLatch.await()
      val reply = HandshakeResponse(ok = true)
      Future.successful(reply)
    }
    override def sendData(req : SendDataRequest) = {
      Master.logger.info("recup data from " + req.ipAddress)
      clientLatch.countDown()
      addData(req.data,req.ipAddress)
      // what to do if a new client send data
      clientLatch.await()
      val reply = SendDataResponse(ok = true)
      Future.successful(reply)

    }
  }

}