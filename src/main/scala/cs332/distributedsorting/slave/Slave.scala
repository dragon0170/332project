package cs332.distributedsorting.slave

import cs332.distributedsorting.common.Util.getMyIpAddress

import java.util.concurrent.{CountDownLatch, TimeUnit}
import org.apache.logging.log4j.scala.Logging
import io.grpc.{ManagedChannel, ManagedChannelBuilder, Server, ServerBuilder}
import cs332.distributedsorting.sorting.{HandshakeRequest, HandshakeResponse, SendSampledDataRequest, SendSampledDataResponse, SendSortedFileRequest, SendSortedFileResponse, SetSlaveServerPortRequest, SetSlaveServerPortResponse, ShufflingGrpc, SortingGrpc}
import cs332.distributedsorting.sorting.SortingGrpc.SortingStub

import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.language.postfixOps
import scala.util.{Failure, Random, Success}
import scala.io.Source
import com.google.protobuf.ByteString
import cs332.distributedsorting.common.KeyOrdering
import io.grpc.stub.StreamObserver
import org.apache.commons.cli.{DefaultParser, Option, Options}

import java.io.{File, FileOutputStream}
import java.net.ServerSocket
import java.util.concurrent.locks.ReentrantLock

object Slave {
  def apply(host: String, port: Int, inputDirectories: Array[String], outputDirectory: String): Slave = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    val stub = SortingGrpc.stub(channel)
    new Slave(channel, stub, inputDirectories, outputDirectory)
  }

  def main(args: Array[String]): Unit = {

    val options = new Options
    options.addOption(
      Option.builder()
        .option("I")
        .desc("Input directories")
        .required
        .hasArgs
        .build
    )
    options.addOption(
      Option.builder()
        .option("O")
        .desc("Output directory")
        .required
        .hasArg
        .build
    )

    val parser = new DefaultParser
    val cmd = parser.parse(options, args)

    val masterEndpoint = cmd.getArgs.headOption
    if (masterEndpoint.isEmpty)
      System.out.println("Master ip:port argument is empty.")
    else {
      val splitedEndpoint = masterEndpoint.get.split(':')
      val client = Slave(splitedEndpoint(0), splitedEndpoint(1).toInt, cmd.getOptionValues("I"), cmd.getOptionValue("O"))
      val done = client.start()
      Await.result(done, Duration.Inf)
      client.shutdown()
    }
  }
}


class Slave private(
  private val channel: ManagedChannel,
  private val stub: SortingStub,
  private val inputDirectories: Array[String],
  private val outputDirectory: String,
) extends Logging {
  class KeyRange(val lowerBound: Array[Byte], val upperBound: Array[Byte]) {
    def inRange(key: Array[Byte]): Boolean = {
      if (key.length != 10) false
      else if (KeyOrdering.compare(key, this.upperBound) > 0) false
      else if (KeyOrdering.compare(key, this.lowerBound) < 0) false
      else true
    }
  }

  val done: Promise[Boolean] = Promise[Boolean]()
  var idToKeyRange: Map[Int, KeyRange] = Map.empty
  var id: Int = 0
  var idToEndpoint: Map[Int, String] = Map.empty
  var serverPort: Int = 0
  var slaveServer: Server = null

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def start(): Future[Boolean] = {
    Future {
      handshake() // maybe handshake function
    }
    done.future
  }

  def handshake(): Unit = {
    val request = HandshakeRequest(ipAddress = getMyIpAddress)
    val response = stub.handshake(request)
    response.onComplete {
      case Success(value) => handleHandshakeResponse(value)
      case Failure(exception) => logger.error("Handshake failed: " + exception)
    }
  }

  def handleHandshakeResponse(response: HandshakeResponse): Unit = {
    assert(response.ok)

    logger.info("Handshake succeeded. slave id: " + response.id)
    this.id = response.id
    val sampledData = getSampledData
    sendSampledData(sampledData)
  }

  def getSampledData: Array[Byte] = {
    val concatenatedData = inputDirectories
      .map(new File(_))
      .flatMap(_.listFiles.filter(_.isFile))
      .map(Source.fromFile(_))
      .flatMap(_.grouped(100).toList.map(x => x.dropRight(90)).take(10000).flatMap(x => x.map(y => y.toByte)).toArray)
//    logger.info(s"concatenated data: ${concatenatedData.mkString("Array(", ", ", ")")}")
    concatenatedData
  }

  def sendSampledData(data: Array[Byte]) : Unit = {
    val request = SendSampledDataRequest(id = this.id, data = ByteString.copyFrom(data))
    logger.info("we have send data")
    val response = stub.sendSampledData(request)
    response.onComplete {
      case Success(value) => {
        handleSendSampledDataResponse(value)
        sortFilesWithKeyRanges()
        startGrpcServer()
        setSlaveServerPort()
      }
      case Failure(exception) => logger.error(s"sendSampledData failed: ${exception}")
    }
  }

  def handleSendSampledDataResponse(response: SendSampledDataResponse): Unit = {
    assert(response.ok)

    logger.info(s"Send Sampled Data succeeded. id to key ranges: ${response.idToKeyRanges.map(entry => (entry._1, (entry._2.lowerBound.toByteArray.toList, entry._2.upperBound.toByteArray.toList)))}")
    this.idToKeyRange = response.idToKeyRanges.map(entry => (entry._1, new KeyRange(lowerBound = entry._2.lowerBound.toByteArray, upperBound = entry._2.upperBound.toByteArray)))
  }

  def sortFilesWithKeyRanges() = {
    val data = loadUnsortedFiles()
    for (d <- data) {
      val sortedResult = sort(d)
      saveWithKeyRanges(sortedResult)
    }
  }

  def loadUnsortedFiles(): List[Source] = {
    val unsortedFiles = inputDirectories
      .map(new File(_))
      .flatMap(_.listFiles.filter(_.isFile))
      .map(Source.fromFile(_))
      .toList
    unsortedFiles
  }

  class SortEntry(val key: Array[Byte], val value: Array[Byte]) {}

  def sort(data: Source): List[SortEntry] = {
    data.grouped(100).toList
      .map(line => {
        val (key, value) = line.splitAt(10)
        new SortEntry(key.map(_.toByte).toArray, value.map(_.toByte).toArray)
      })
      .sortBy(_.key)(KeyOrdering)
  }

  def saveWithKeyRanges(sortedResult: List[SortEntry]): Unit = {
    val idToKeyIter = this.idToKeyRange.iterator
    var (id, keyRange) = idToKeyIter.next()
    var filename = s"${this.outputDirectory}/${id}_${Random.alphanumeric.take(10).mkString}"
//    logger.info(s"filename: ${filename}")
    var file = new File(filename)
    var outputStream = new FileOutputStream(file)

    for (result <- sortedResult) {
      if (KeyOrdering.compare(result.key, keyRange.upperBound) > 0) {
        outputStream.close()
        val (newId, newKeyRange) = idToKeyIter.next()
        id = newId
        keyRange = newKeyRange
        filename = s"${this.outputDirectory}/${id}_${Random.alphanumeric.take(10).mkString}"
//        logger.info(s"filename: ${filename}")
        file = new File(filename)
        outputStream = new FileOutputStream(file)
      }
      outputStream.write(result.key)
      outputStream.write(result.value)
    }

    outputStream.close()
  }

  private def findRandomAvailablePort: Int = {
    val socket = new ServerSocket(0)
    val availablePort = socket.getLocalPort
    socket.close()
    availablePort
  }

  def startGrpcServer(): Unit = {
    this.serverPort = findRandomAvailablePort
    // start grpc server with Shuffling service and save opened port to serverPort variable
    slaveServer = ServerBuilder.forPort(this.serverPort).addService(ShufflingGrpc.bindService(new ShufflingImpl, ExecutionContext.global)).build.start
    logger.info("Server started, listening on " + this.serverPort.toString())
  }

  def stopServer(): Unit = {
    if (slaveServer != null) {
      slaveServer.shutdown()
    }
  }

  def setSlaveServerPort(): Unit = {
    val request = SetSlaveServerPortRequest(id = this.id, port = this.serverPort)
    val response = stub.setSlaveServerPort(request)
    response.onComplete {
      case Success(value) => {
        handleSetSlaveServerPortResponse(value)
        shuffleSortedFiles()
      }
      case Failure(exception) => logger.error("SetSlaveServerPort failed: " + exception)
    }
  }

  def handleSetSlaveServerPortResponse(value: SetSlaveServerPortResponse): Unit = {
    this.idToEndpoint = value.idToServerEndpoint
    logger.info(this.idToEndpoint)
  }

  def shuffleSortedFiles(): Unit = {
    for ((id, endpoint) <- this.idToEndpoint.filter(_._1 != this.id)) {
      logger.info(s"Start sending files from slave id: ${this.id} to slave id: $id")
      val files = getAllFilesOfId(id)
      val finishLatch = new CountDownLatch(1)
      var sendLatch = new CountDownLatch(1)

      logger.info(s"endpiont: $endpoint")
      val splitedEndpoint = endpoint.split(':')
      val shufflingChannel = ManagedChannelBuilder.forAddress(splitedEndpoint(0), splitedEndpoint(1).toInt).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
      val shufflingStub = ShufflingGrpc.stub(shufflingChannel)
      val requestObserver = shufflingStub.sendSortedFiles(new StreamObserver[SendSortedFileResponse] {
        override def onNext(value: SendSortedFileResponse): Unit = {
          sendLatch.countDown()
        }

        override def onError(t: Throwable): Unit = {
          logger.error(s"sendSortedFiles failed: ${t.toString}")
          finishLatch.countDown()
        }

        override def onCompleted(): Unit = {
          finishLatch.countDown()
        }
      })
      for (file <- files) {
        requestObserver.onNext(SendSortedFileRequest(file = ByteString.copyFrom(file.toList.map(x => x.toByte).toArray)))
        sendLatch.await()
        sendLatch = new CountDownLatch(1)
      }
      requestObserver.onCompleted()
      finishLatch.await()
      logger.info(s"Finish sending files from slave id: ${this.id} to slave id: $id")
    }
  }

  def getAllFilesOfId(id: Int): List[Source] = {
    val sortedFiles = new File(outputDirectory)
      .listFiles
      .filter(_.isFile)
      .filter(_.getName.startsWith(s"${id}_"))
      .map(Source.fromFile(_))
      .toList
    sortedFiles
  }

  private class ShufflingImpl extends ShufflingGrpc.Shuffling {
    override def sendSortedFiles(responseObserver: StreamObserver[SendSortedFileResponse]): StreamObserver[SendSortedFileRequest] = {
      val requestObserver = new StreamObserver[SendSortedFileRequest] {
        override def onNext(value: SendSortedFileRequest): Unit = {
          saveSortedFile(value.file.toByteArray)
          responseObserver.onNext(SendSortedFileResponse(ok = true))
        }

        override def onError(t: Throwable): Unit = {
          logger.error(s"handling sendSortedFiles error: ${t.toString}")
        }

        override def onCompleted(): Unit = {
          responseObserver.onCompleted()
        }
      }
      requestObserver
    }
  }

  def saveSortedFile(file: Array[Byte]): Unit = {
    val filename = s"${this.outputDirectory}/${this.id}_${Random.alphanumeric.take(10).mkString}_shuffled"
    logger.info(s"save received file: $filename")
    val newFile = new File(filename)
    val outputStream = new FileOutputStream(newFile)
    outputStream.write(file)
    outputStream.close()
  }
}