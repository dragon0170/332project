package cs332.distributedsorting.master

import org.apache.logging.log4j.scala.Logging
import io.grpc.{Server, ServerBuilder}
import cs332.distributedsorting.common.Util.getMyIpAddress
import cs332.distributedsorting.sorting.{HandshakeRequest, HandshakeResponse, Part, SendDataRequest, SendDataResponse, SortingGrpc}
import com.google.protobuf.ByteString
import cs332.distributedsorting.common.KeyOrdering
import cs332.distributedsorting.master.SortingStates.{Handshaking, Initial, Sampling, Sorting, SortingState}

import scala.collection.mutable.Map
import java.util.concurrent.CountDownLatch
import scala.concurrent.ExecutionContext.Implicits.global
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
  var keyStart: Array[Byte] = _
  var keyEnd: Array[Byte] = _
  var serverPort: Int = _
  override def toString: String = ip
}

object SortingStates extends Enumeration {
  type SortingState = Value
  val Initial, Handshaking, Sampling, Sorting, Shuffling, Merging, End = Value
}

class Master(executionContext: ExecutionContext, val numClient: Int) extends Logging { self =>
  private[this] var server: Server = null
  private var clientLatch: CountDownLatch = _
  var state: SortingState = Initial
  var slaves: Vector[SlaveClient] = Vector.empty
  var data : List[Array[Byte]] = Nil
  var partition : Map[String,(Array[Byte], Array[Byte])] = Map.empty
  var count = 0

  def start(): Unit = {
    server = ServerBuilder.forPort(Master.port).addService(SortingGrpc.bindService(new SortingImpl, executionContext)).build.start
    logger.info("Server numClient: " + self.numClient)
    logger.info("Server started, listening on " + Master.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
    transitionToHandshaking()
  }

  def transitionToHandshaking(): Unit = {
    logger.info("Transition to handshaking")
    assert(this.state == Initial)
    this.clientLatch = new CountDownLatch(this.numClient)
    this.state = Handshaking
  }

  def transitionToSampling(): Unit = {
    logger.info("Transition to sampling")
    assert(this.state == Handshaking)
    this.clientLatch.await()
    this.clientLatch = new CountDownLatch(this.numClient)
    this.state = Sampling
  }

  def transitionToSorting(): Unit = {
    logger.info("Transition to sorting")
    assert(this.state == Sampling)
    this.clientLatch = new CountDownLatch(this.numClient)
    this.state = Sorting
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

  private def addNewSlave(ipAddress: String): Int = {
    this.synchronized {
      val slaveId = this.slaves.length
      this.slaves = this.slaves :+ new SlaveClient(this.slaves.length, ipAddress)
      if (this.slaves.length == this.numClient) {
        printSlaveIpAddresses()
        Future {
          transitionToSampling()
        }
      }
      slaveId
    }
  }

  private def printSlaveIpAddresses(): Unit = {
    System.out.println(this.slaves.mkString(", "))
  }

  private def addData(data_ : Array[Byte],ipAddress : String) : Boolean = {
    this.synchronized({
      /*if (this.slaves.toList.filter(x =>x.ip == ipAddress).isEmpty)
      Master.logger.info("we receive data from a client we did not register")
        return false*/
      this.count +=1
      this.data = this.data ++ data_.grouped(10).toList
      if(this.count == numClient)
        logger.info("we receive all the data")
      return true
    })
  }

  private def createPartition(): Map[String, (Array[Byte], Array[Byte])] = {
    assert(this.count == this.numClient)
    var mindata : Array[Byte] = Array(0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte)
    var maxdata : Array[Byte] = Array(-1.toByte,-1.toByte,-1.toByte,-1.toByte,-1.toByte,-1.toByte,-1.toByte,-1.toByte,-1.toByte,-1.toByte)
    // we first need to sort the data list
    this.data = this.data.sorted(KeyOrdering)
    // then we can create the partiton
    if (this.count == 1){
      this.partition.put(slaves(0).toString(), (mindata, maxdata))
    }
    else {
      val range:Int =(data.length/this.count) 
      var loop = 0
      for (slave <- this.slaves.toList){
        if (loop == 0){
          this.partition.put(slave.toString(), (mindata, this.data((loop+1)*range -1)))
        }
        else if (loop == count-1){
          this.partition.put(slave.toString(), (data((loop)*range), maxdata))
        }
        else{
          this.partition.put(slave.toString(), (data(loop*range), data((loop+1)*range -1)))
        } 
        loop +=1
      }
    }
    return this.partition
  }

  private class SortingImpl extends SortingGrpc.Sorting {
    override def handshake(req: HandshakeRequest) = {
      assert(self.state == Handshaking)
      logger.info("Handshake from " + req.ipAddress)
      val slaveId = addNewSlave(req.ipAddress)
      clientLatch.countDown()
      clientLatch.await()
      val reply = HandshakeResponse(ok = true, id = slaveId)
      Future.successful(reply)
    }
    override def sendData(req : SendDataRequest) = {
      assert(self.state == Sampling)
      logger.info("recup data from " + req.ipAddress)
      addData(req.data.toByteArray(),req.ipAddress)
      clientLatch.countDown()
      clientLatch.await()
      logger.info("Thread : "+ Thread.currentThread().getName() + "is running")
      // what to do if a new client send data
      // we have to reply with partition
      val temp :Map[String,Part] = createPartition().map(x=>(x._1,Part(lowerbound = ByteString.copyFrom(x._2._1),upperbound = ByteString.copyFrom(x._2._2))))
      val reply = SendDataResponse(ok = true, partition = temp.toMap)
      Future.successful(reply)

    }
  }

}