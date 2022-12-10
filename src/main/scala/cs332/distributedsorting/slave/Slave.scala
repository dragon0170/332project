package cs332.distributedsorting.slave

import cs332.distributedsorting.common.Util.getMyIpAddress

import java.util.concurrent.TimeUnit
import org.apache.logging.log4j.scala.Logging
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import cs332.distributedsorting.sorting.{HandshakeRequest, HandshakeResponse, SendSampledDataRequest, SendSampledDataResponse, SortingGrpc}
import cs332.distributedsorting.sorting.SortingGrpc.SortingStub

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration}
import scala.language.postfixOps
import scala.util.{Failure, Success}
import scala.io.Source
import com.google.protobuf.ByteString
import org.apache.commons.cli.{DefaultParser, Option, Options}

import java.io.File

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
  class KeyRange(val lowerBound: Array[Byte], val upperBound: Array[Byte]) {}

  val done: Promise[Boolean] = Promise[Boolean]()
  var idToKeyRange: Map[Int, KeyRange] = Map.empty
  var id: Int = 0

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
    logger.info(s"concatenated data: ${concatenatedData.mkString("Array(", ", ", ")")}")
    concatenatedData
  }

  def sendSampledData(data: Array[Byte]) : Unit = {
    val request = SendSampledDataRequest(id = this.id, data = ByteString.copyFrom(data))
    logger.info("we have send data")
    val response = stub.sendSampledData(request)
    response.onComplete {
      case Success(value) => handleSendSampledDataResponse(value)
      case Failure(exception) => logger.error(s"sendSampledData failed: ${exception}")
    }
  }

  def handleSendSampledDataResponse(response: SendSampledDataResponse): Unit = {
    assert(response.ok)

    logger.info(s"Send Sampled Data succeeded. id to key ranges: ${response.idToKeyRanges.map(entry => (entry._1, (entry._2.lowerBound.toByteArray.toList, entry._2.upperBound.toByteArray.toList)))}")
    this.idToKeyRange = response.idToKeyRanges.map(entry => (entry._1, new KeyRange(lowerBound = entry._2.lowerBound.toByteArray, upperBound = entry._2.upperBound.toByteArray)))
  }
}