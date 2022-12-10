package cs332.distributedsorting.master

import org.apache.logging.log4j.scala.Logging
import io.grpc.{Server, ServerBuilder}
import cs332.distributedsorting.sorting.{SortingGrpc, SetSlaveServerPortRequest, SetSlaveServerPortResponse}
import cs332.distributedsorting.common.Util.getMyIpAddress

import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}
import java.sql.Savepoint

object Master {
  /*
  def main(args: Array[String]): Unit = {
    val numClient = args.headOption
    if (numClient.isEmpty) return

    val server = new Master(ExecutionContext.global, numClient.get.toInt)
    server.start()
    server.printEndpoint()
    server.blockUntilShutdown()
  }*/


  private val port = 5190
}

class SlaveClient(val id: Int, val ip: String, var port : Int) {
  override def toString: String = ip
}

class Master(executionContext: ExecutionContext, val numClient: Int, forTest : Vector[SlaveClient]) extends Logging { self =>
  private[this] var server: Server = null
  private val clientLatch: CountDownLatch = new CountDownLatch(numClient)
  var slaves: Vector[SlaveClient] = forTest
  var idToEndpoint :scala.collection.Map[Int, String] = scala.collection.Map.empty

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
      this.slaves = this.slaves :+ new SlaveClient(this.slaves.length, ipAddress,0)
      if (this.slaves.length == this.numClient) printSlaveIpAddresses()
    }
  }

  private def printSlaveIpAddresses(): Unit = {
    System.out.println(this.slaves.mkString(", "))
  }

  def setSlavePort(slaveId: Int, serverPort: Int) = {
    // find slave in slaves and update serverPort member
    // if all slaves port is received, call transitionToMerging function
    slaves.find(p=> p.id == slaveId) match {
      case None => logger.info("id does not match") 
      self.stop()
      case Some(value) => value.port = serverPort
    }

  }
    def getIdToEndpoint() : Map[Int, String] = {
    for (slave <- slaves){
      val ipPort :String = slave.ip + ":" + slave.port.toString()
      idToEndpoint +=  (slave.id -> ipPort)
    }
    return idToEndpoint.toMap
  }


  private class SortingImpl extends SortingGrpc.Sorting {
    
    
    override def setSlaveServerPort(req : SetSlaveServerPortRequest) = {
       // check if current state is shuffling
      setSlavePort(req.id, req.port)
      clientLatch.countDown()
      clientLatch.await()
      val idToEndpoint = getIdToEndpoint()
      val reply = SetSlaveServerPortResponse(ok = true, idToServerEndpoint = idToEndpoint)
      Future.successful(reply)
    }
  }

}