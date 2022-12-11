package cs332.distributedsorting.master

import org.apache.logging.log4j.scala.Logging
import io.grpc.{Server, ServerBuilder}
import cs332.distributedsorting.common.Util.{findRandomAvailablePort, getMyIpAddress}
import cs332.distributedsorting.sorting.{HandshakeRequest, HandshakeResponse, NotifyMergingCompletedRequest, NotifyMergingCompletedResponse, SendNumFilesRequest, SendNumFilesResponse, SendSampledDataRequest, SendSampledDataResponse, SetSlaveServerPortRequest, SetSlaveServerPortResponse, SortingGrpc}
import com.google.protobuf.ByteString
import cs332.distributedsorting.common.KeyOrdering
import cs332.distributedsorting.master.SortingStates.{End, Handshaking, Initial, Merging, Sampling, Shuffling, Sorting, SortingState}
import cs332.distributedsorting.sorting.SendSampledDataResponse.KeyRanges

import scala.collection.mutable.Map
import java.util.concurrent.CountDownLatch
import scala.concurrent.{ExecutionContext, Future}

object Master {
  def main(args: Array[String]): Unit = {
    val numClient = args.headOption
    if (numClient.isEmpty) return

    var port: Int = 0
    args.find(arg => arg == "DEBUG") match {
      case Some(_) => port = 50051
      case None => port = Master.randomPort
    }

    val server = new Master(ExecutionContext.global, numClient.get.toInt, port)
    server.start()
    server.printEndpoint()
    server.blockUntilShutdown()
  }

  private val randomPort = findRandomAvailablePort
}

class SlaveClient(val id: Int, val ip: String) {
  var keyStart: Array[Byte] = _
  var keyEnd: Array[Byte] = _
  var serverPort: Int = 0
  var gotSampledData: Boolean = false
  var numFile: Int = 0
  var gotNumFile: Boolean = false
  var ended: Boolean = false
  override def toString: String = ip
}

object SortingStates extends Enumeration {
  type SortingState = Value
  val Initial, Handshaking, Sampling, Sorting, Shuffling, Merging, End = Value
}

class Master(executionContext: ExecutionContext, val numClient: Int, val port: Int) extends Logging { self =>
  private[this] var server: Server = null
  private val handshakeLatch: CountDownLatch = new CountDownLatch(numClient)
  private val sampleLatch: CountDownLatch = new CountDownLatch(numClient)
  private val shuffleLatch: CountDownLatch = new CountDownLatch(numClient)
  private val mergeLatch: CountDownLatch = new CountDownLatch(numClient)
  var state: SortingState = Initial
  var slaves: Vector[SlaveClient] = Vector.empty
  var sampledKeyData: List[Array[Byte]] = Nil
  var idToKeyRanges: Map[Int, KeyRanges] = Map.empty
  var idToEndpoint: Map[Int, String] = Map.empty

  def start(): Unit = {
    server = ServerBuilder.forPort(this.port).addService(SortingGrpc.bindService(new SortingImpl, executionContext)).build.start
    logger.info("Server numClient: " + self.numClient)
    logger.info("Server started, listening on " + this.port)
    sys.addShutdownHook {
      logger.info("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      logger.info("*** server shut down")
    }
    transitionToHandshaking()
  }

  def transitionToHandshaking(): Unit = {
    logger.info("Transition to handshaking")
    assert(this.state == Initial)
    this.state = Handshaking
  }

  def transitionToSampling(): Unit = {
    logger.info("Transition to sampling")
    assert(this.state == Handshaking)
    this.state = Sampling
  }

  def transitionToSorting(): Unit = {
    logger.info("Transition to sorting")
    assert(this.state == Sampling)
    this.state = Sorting
  }

  def transitionToShuffling(): Unit = {
    logger.info("Transition to shuffling")
    assert(this.state == Sorting)
    this.state = Shuffling
  }

  def transitionToMerging(): Unit = {
    logger.info("Transition to merging")
    assert(this.state == Shuffling)
    this.state = Merging
  }

  def transitionToEnd(): Unit = {
    logger.info("Transition to end")
    assert(this.state == Merging)
    this.state = End
  }

  private def printEndpoint(): Unit = {
    System.out.println(getMyIpAddress + ":" + this.port)
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
        transitionToSampling()
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
        transitionToSorting()
        transitionToShuffling()
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
    // We do not need sampled key data anymore, so it can be garbage collected
    this.sampledKeyData = Nil
  }

  def setSlavePort(slaveId: Int, serverPort: Int): Unit = {
    // if all slaves port is received, call transitionToMerging function
    this.synchronized {
      slaves.find(p => p.id == slaveId) match {
        case None => logger.error("id does not match")
        case Some(value) =>
          assert(value.serverPort == 0)
          value.serverPort = serverPort
      }
      if (this.slaves.count(_.serverPort != 0) == this.numClient) {
        logger.info("we receive all the slave port")
        setIdToEndpoint()
        transitionToMerging()
      }
    }
  }

  def setIdToEndpoint(): Unit = {
    for (slave <- slaves) {
      val ipPort = s"${slave.ip}:${slave.serverPort}"
      idToEndpoint += (slave.id -> ipPort)
    }
    this.idToEndpoint = idToEndpoint
  }

  def setNumFiles(id: Int, num: Int): Unit = {
    this.synchronized {
      val slave: Option[SlaveClient] = slaves.find(x => x.id == id)
      slave match {
        case None => logger.error("id does not match")
        case Some(value) =>
          value.numFile = num
          value.gotNumFile = true
      }
      if (slaves.forall(x => x.gotNumFile)) {
        transitionToEnd()
      }
    }
  }

  def getStartIndexAndLength(id: Int): (Int, Int) = {
    val totalFiles: Int = slaves.foldLeft(0)((x, y) => x + y.numFile)
    val length: Int = totalFiles / slaves.length

    // we give less files to the last slaves if the totalNumberOfFiles is not a multiple of the number of slaves
    if (id == slaves.length - 1 && length * slaves.length != totalFiles)
      (id * length, length + (totalFiles % slaves.length))
    else
      (id * length, length)
  }

  def setSortingFinished(id: Int) {
    this.synchronized {
      val slave: Option[SlaveClient] = slaves.find(x => x.id == id)
      slave match {
        case None => logger.error("id does not match ")
        case Some(value) => value.ended = true
      }
      if (slaves.forall(x => x.ended)) {
        server.shutdown()
      }
    }
  }

  private class SortingImpl extends SortingGrpc.Sorting {
    override def handshake(req: HandshakeRequest) = {
      assert(self.state == Handshaking)
      logger.info("Handshake from " + req.ipAddress)
      val slaveId = addNewSlave(req.ipAddress)
      handshakeLatch.countDown()
      handshakeLatch.await()
      val reply = HandshakeResponse(ok = true, id = slaveId)
      Future.successful(reply)
    }
    override def sendSampledData(req : SendSampledDataRequest) = {
      assert(self.state == Sampling)
      logger.info("sampled data from " + req.id)
      addSampledData(req.id, req.data.toByteArray)
      sampleLatch.countDown()
      sampleLatch.await()
      val reply = SendSampledDataResponse(ok = true, idToKeyRanges = self.idToKeyRanges.toMap)
      Future.successful(reply)
    }

    override def setSlaveServerPort(req: SetSlaveServerPortRequest): Future[SetSlaveServerPortResponse] = {
      assert(self.state == Shuffling)
      logger.info(s"slave server port from ${req.id}, server port: ${req.port}")
      setSlavePort(req.id, req.port)
      shuffleLatch.countDown()
      shuffleLatch.await()
      val reply = SetSlaveServerPortResponse(ok = true, idToServerEndpoint = self.idToEndpoint.toMap)
      Future.successful(reply)
    }

    override def sendNumFiles(req: SendNumFilesRequest) = {
      assert(state == SortingStates.Merging)
      setNumFiles(req.id, req.num)
      mergeLatch.countDown()
      mergeLatch.await()
      val (startIndex, length) = getStartIndexAndLength(req.id)
      val reply = SendNumFilesResponse(ok = true, startIndex = startIndex, length = length)
      Future.successful(reply)
    }

    override def notifyMergingCompleted(req: NotifyMergingCompletedRequest) = {
      logger.info(s"slave merge completed. id: ${req.id}")
      assert(self.state == SortingStates.End)
      setSortingFinished(req.id)
      val reply = NotifyMergingCompletedResponse(ok = true)
      logger.info(s"slave merge completed reply. id: ${req.id}")
      Future.successful(reply)
    }
  }

}