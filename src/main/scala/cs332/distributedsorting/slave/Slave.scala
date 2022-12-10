package cs332.distributedsorting.slave

import cs332.distributedsorting.common.Util.getMyIpAddress

import java.util.concurrent.TimeUnit
import org.apache.logging.log4j.scala.Logging
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import cs332.distributedsorting.sorting.{HandshakeRequest, SortingGrpc, SendDataRequest}
import cs332.distributedsorting.sorting.SortingGrpc.SortingStub

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps
import scala.util.{Failure, Success}
import scala.io.Source
import com.google.protobuf.ByteString

object Slave {
  def apply(host: String, port: Int): Slave = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    val stub = SortingGrpc.stub(channel)
    new Slave(channel, stub)
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
      } catch {
        case e : Exception => {
          client.shutdown()
          return
        }
      }
      // suppose args(1) is  the path to data file generated with gensort
      val path = args.lastOption
      if (path.isEmpty) {
        System.out.println("Path to data file is empty.")
        client.shutdown()
      } else {
        try {
          client.sendData(path.get)
        } finally {
          client.shutdown()
        }
      }
    }
  }
}


class Slave private(
  private val channel: ManagedChannel,
  private val stub: SortingStub,
) extends Logging {
  var partition: Map[String, (Array[Byte], Array[Byte])] = Map.empty

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def handshake(): Unit = {
    val request = HandshakeRequest(ipAddress = getMyIpAddress)
    val response = stub.handshake(request)
    response.onComplete {
      case Success(value) => logger.info("Handshake succeeded: " + value.ok)
      case Failure(exception) => logger.error("Handshake failed: " + exception)
    }
    Await.ready(response, 10 seconds)
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