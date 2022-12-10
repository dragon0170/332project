package cs332.distributedsorting.slave

import cs332.distributedsorting.common.Util.getMyIpAddress

import java.util.concurrent.TimeUnit
import org.apache.logging.log4j.scala.Logging
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import cs332.distributedsorting.sorting.{HandshakeRequest, HandshakeResponse, SendDataRequest, SortingGrpc}
import cs332.distributedsorting.sorting.SortingGrpc.SortingStub

import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, DurationInt}
import scala.language.postfixOps
import scala.util.{Failure, Success}
import scala.io.Source
import com.google.protobuf.ByteString
import org.apache.commons.cli.{DefaultParser, Option, Options}

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
  val done: Promise[Boolean] = Promise[Boolean]()
  var partition: Map[String, (Array[Byte], Array[Byte])] = Map.empty
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
  }

  def sendData(path : String) : Unit = {
    val fSource = Source.fromFile(path)
    val data = fSource.grouped(100).toList.map(x=>x.dropRight(90)).take(10).map(x=>x.map(y=>y.toByte)).flatten.toArray
    val request = SendDataRequest(ipAddress = "temp", data = ByteString.copyFrom(data))
    logger.info("we have send data")
    val response = stub.sendData(request)
    response.onComplete {
      case Success(value) => {
        if (value.ok) {
          this.partition = value.partition.map(x => (x._1, (x._2.lowerbound.toByteArray(), x._2.upperbound.toByteArray())))
          System.out.println(partition.map(x => (x._1, (x._2._1.toList, x._2._2.toList))))
        } else {
          logger.error("create partition failed")
        }
      }
      case Failure(exception) => logger.error(s"RPC failed: ${exception}")
    }

  }
}