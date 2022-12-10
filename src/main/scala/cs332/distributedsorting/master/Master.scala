package cs332.distributedsorting.master

import org.apache.logging.log4j.scala.Logging
import io.grpc.{Server, ServerBuilder}
import cs332.distributedsorting.common.Util.getMyIpAddress
import cs332.distributedsorting.sorting.{HandshakeRequest, HandshakeResponse, SendSampledDataRequest, SendSampledDataResponse, SortingGrpc}
import com.google.protobuf.ByteString
import cs332.distributedsorting.common.KeyOrdering
import cs332.distributedsorting.master.SortingStates.{Handshaking, Initial, Sampling, Sorting, SortingState}
import cs332.distributedsorting.sorting.SendSampledDataResponse.KeyRanges

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
  var gotSampledData: Boolean = false
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
  var sampledKeyData: List[Array[Byte]] = Nil
  var idToKeyRanges: Map[Int, KeyRanges] = Map.empty

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
    this.clientLatch.await()
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

  private def addSampledData(id: Int, sampledData : Array[Byte]): Unit = {
    this.synchronized({
      val slave = this.slaves.find(_.id == id).get
      assert(!slave.gotSampledData)
      slave.gotSampledData = true

      this.sampledKeyData = this.sampledKeyData ++ sampledData.grouped(10).toList
      if (this.slaves.count(_.gotSampledData) == this.numClient) {
        logger.info("we receive all the sampled data")
        createPartition()
        Future {
          transitionToSorting()
        }
      }
    })
  }

  private def createPartition(): Unit = {
    val mindata: Array[Byte] = Array(Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue, Byte.MinValue)
    val maxdata: Array[Byte] = Array(Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue, Byte.MaxValue)
    val sortedKeyData = this.sampledKeyData.sorted(KeyOrdering)

    val partition: Map[Int,(Array[Byte], Array[Byte])] = Map.empty
    if (this.slaves.length == 1) {
      partition.put(slaves(0).id, (mindata, maxdata))
    } else {
      val range: Int = sortedKeyData.length / this.slaves.length
      var loop = 0
      for (slave <- this.slaves.toList) {
        if (loop == 0) {
          val bytes = sortedKeyData((loop + 1) * range).clone()
          bytes.update(9, bytes(9).-(1).toByte)
          partition.put(slave.id, (mindata, bytes))
        } else if (loop == this.slaves.length - 1) {
          partition.put(slave.id, (sortedKeyData(loop * range), maxdata))
        } else {
          val bytes = sortedKeyData((loop + 1) * range).clone()
          bytes.update(9, bytes(9).-(1).toByte)
          partition.put(slave.id, (sortedKeyData(loop * range), bytes))
        } 
        loop +=1
      }
    }
    this.idToKeyRanges = partition.map(x=>(x._1, KeyRanges(lowerBound = ByteString.copyFrom(x._2._1), upperBound = ByteString.copyFrom(x._2._2))))
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
    override def sendSampledData(req : SendSampledDataRequest) = {
      assert(self.state == Sampling)
      logger.info("sampled data from " + req.id)
      addSampledData(req.id, req.data.toByteArray)
      clientLatch.countDown()
      clientLatch.await()
      val reply = SendSampledDataResponse(ok = true, idToKeyRanges = self.idToKeyRanges.toMap)
      Future.successful(reply)
    }
  }

}