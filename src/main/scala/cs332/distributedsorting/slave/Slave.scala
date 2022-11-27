package cs332.distributedsorting.slave

import cs332.distributedsorting.common.Util.getMyIpAddress
import scala.io.Source 
import java.util.concurrent.TimeUnit
import java.util.logging.{Level, Logger}
import com.google.protobuf.ByteString
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import cs332.distributedsorting.sorting.{HandshakeRequest, SortingGrpc, SendDataRequest, SendDataResponse, Part}
import cs332.distributedsorting.sorting.SortingGrpc.SortingBlockingStub

object Slave {
  def apply(host: String, port: Int): Slave = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    val blockingStub = SortingGrpc.blockingStub(channel)
    new Slave(channel, blockingStub)
  }

  def main(args: Array[String]): Unit = {
    val masterEndpoint = args.headOption
    if (masterEndpoint.isEmpty)
      System.out.println("Master ip:port argument is empty.")
    else {
      System.out.println("Try to handshake with master: " + masterEndpoint.get)
      val splitedEndpoint = masterEndpoint.get.split(':')
      val client = Slave(splitedEndpoint(0), splitedEndpoint(1).toInt)
      try {
        client.handshake()
      }
      catch{
        case e : Exception => {
          client.shutdown()
          return
        }
      }
      System.out.println("handshake")
        // suppose args(1) is  the path to data file generated with gensort
      val path = args.lastOption
      if (path.isEmpty){
        System.out.println("Path to data file is empty.")
        client.shutdown()
      }
      else{
        System.out.println("Try to send data to master")
        val fSource = Source.fromFile(path.get)
        val keyList = fSource.grouped(100).toList.map(x=>x.dropRight(90)).take(10).map(x=>x.map(y=>y.toByte)).flatten.toArray
        try{
          client.sendData(keyList)
        }finally{
        client.shutdown()
        }
      }
    }
  }
}


class Slave private(
  private val channel: ManagedChannel,
  private val blockingStub: SortingBlockingStub
)
 {
  var partition: Map[String, (Array[Byte], Array[Byte])] = Map.empty
  private[this] val logger = Logger.getLogger(classOf[Slave].getName)

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }

  def handshake(): Unit = {
    val request = HandshakeRequest(ipAddress = getMyIpAddress)
    try {
      val response = blockingStub.handshake(request)
      logger.info("Handshake: " + response.ok)
    }
    catch {
      case e: StatusRuntimeException =>
        logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus)
      }
  }

  def sendData(data :Array[Byte]) : Unit = {
    val request = SendDataRequest(ipAddress = getMyIpAddress, data = ByteString.copyFrom(data))
    System.out.println("we have send data")
    try {
      val response: SendDataResponse = blockingStub.sendData(request)
      if (response.ok){
        this.partition = response.partition.map(x=> (x._1, (x._2.lowerbound.toByteArray(), x._2.upperbound.toByteArray())))
        System.out.println(partition)
      }
      else {
        logger.info("master does not succeed to create a partition")
      }
    }
    catch {
      case e : StatusRuntimeException => 
        logger.log(Level.WARNING,"RPC failed: {0}", e.getStatus)
    }

  }








}