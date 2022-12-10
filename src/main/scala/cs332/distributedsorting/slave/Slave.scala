package cs332.distributedsorting.slave
import org.apache.logging.log4j.scala.Logging
import cs332.distributedsorting.common.Util.getMyIpAddress
import cs332.distributedsorting.common.KeyRange
import java.util.concurrent.TimeUnit
import org.apache.logging.log4j.scala.Logging
import io.grpc.{ManagedChannel, ManagedChannelBuilder, StatusRuntimeException}
import cs332.distributedsorting.sorting.{SortingGrpc, SetSlaveServerPortResponse, SetSlaveServerPortRequest, SendSortedFileRequest, SendSortedFileResponse}
import cs332.distributedsorting.sorting.SortingGrpc.{SortingBlockingStub, SortingStub}
import cs332.distributedsorting.sorting.ShufflingGrpc._
import cs332.distributedsorting.sorting.ShufflingGrpc
import io.grpc.{Server, ServerBuilder}
import scala.io.Source
import java.io.FileWriter
import java.io.File
import scala.io.BufferedSource
import scala.reflect.macros.NonemptyAttachments
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock
import io.grpc.stub.StreamObserver
import com.google.protobuf.ByteString
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.ExecutionContext
import util.control.Breaks._

object Slave {
  def apply(host: String, port: Int, inputPath : String, id : Int, serverPort : Int, idToKeyRangeForTest : Map[Int, KeyRange] ): Slave = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
    val blockingStub = SortingGrpc.blockingStub(channel)
    //val asynchronousStub = ShufflingGrpc.stub(channel)
    new Slave(channel, blockingStub, id , idToKeyRangeForTest, Map.empty, serverPort, inputPath)
  }

  def main(args: Array[String]): Unit = {
    val masterEndpoint = args.headOption
    if (masterEndpoint.isEmpty)
      System.out.println("Master ip:port argument is empty.")
    else {
      val splitedEndpoint = masterEndpoint.get.split(':')
      val client = Slave(splitedEndpoint(0), splitedEndpoint(1).toInt,"/Users/mathisayma_1/SoftwareDesignMethods/test.txt", 0, 5052, Map.empty)
      try {
        //client.handshake()
      } finally {
        //client.shutdown()
      }
    }
  }
}

class Slave(
  val channel : ManagedChannel,
  //val slaveStub : ShufflingStub,
  val blockingStub : SortingBlockingStub,
  var id: Int,
  val idToKeyRange: scala.collection.Map[Int, KeyRange], // partition [Id -> KeyRange]
  var idToEndpoint: scala.collection.Map[Int, String],  //[Id ->ip adress:port number]
  val serverPort: Int,
  val inputPath : String,
  var slaveServer :Server = null, 
  var index : Int = 0
) extends Logging {
  

  def startGrpcServer() = {
    // start grpc server with Shuffling service and save opened port to serverPort variable
    slaveServer = ServerBuilder.forPort(serverPort).addService(ShufflingGrpc.bindService(new ShufflingImpl, ExecutionContext.global)).build.start
    logger.info("Server started, listening on " + serverPort.toString())
  }

  def stopServer(): Unit = {
    if (slaveServer != null) {
      slaveServer.shutdown()
    }
  }

  def shutdown(): Unit = {
    channel.shutdown.awaitTermination(5, TimeUnit.SECONDS)
  }


  def handleSetSlaveServerPortResponse(value : SetSlaveServerPortResponse) = {
    idToEndpoint = value.idToServerEndpoint
  }

  def getAllFilesOfId(id: Int): List[Array[Byte]]= {
    val file = Source.fromFile(inputPath)
    System.out.println("-----------------------------------------getAllFilesOfId-----------------------------------")
    //return file.grouped(100).toList.filter(x=> idToKeyRange(id).inRange(x.dropRight(90).map(z=>z.toByte).toArray)).toList.map(x=>x.map(y=>y.toByte).toArray)
    return file.grouped(100).toList.map(x=>x.map(y=>y.toByte).toArray)
  }



  def setSlaveServerPort() = {
    val request = SetSlaveServerPortRequest(id = this.id, port = this.serverPort)
    try{
      val response = blockingStub.setSlaveServerPort(request)
      handleSetSlaveServerPortResponse(response)
      System.out.println("Slaves id :" + id.toString + "receive idToEndPoint : " + idToEndpoint.toString())
      shuffleSortedFiles()
      //sendNumSortedFiles()
      shutdown()
    }catch{
      case e : StatusRuntimeException => 
        logger.warn(s"RPC failed: ${e.getStatus}")
    }
  }

  def sendSortedFiles(lock : ReentrantLock, finishLatch:CountDownLatch, slaveStub: ShufflingStub):StreamObserver[SendSortedFileRequest] = {
    val requestObserver = slaveStub.sendSortedFiles(new StreamObserver[SendSortedFileResponse]() {
          override def onNext(value: SendSortedFileResponse) = {
            lock.unlock()
            // what are we supposed to do if value = false 
          }
          override def onError(t: Throwable) = {
            System.out.println(" I'm slave number : " +  id.toString())
            logger.info("Error : " +t.toString())
            finishLatch.countDown()
          }
          override def onCompleted() = {
            finishLatch.countDown()
          }
      })
    return requestObserver
  }



    
     
  def shuffleSortedFiles() = {
    for (id <- idToKeyRange.keySet) {
      breakable{
        // send to other slave server without its own id
        if (id == this.id){
          break()
        }
        val files = getAllFilesOfId(id)
        System.out.println("I'm slaves id : " + this.id.toString + " and I'm sending to slave id : " + id.toString +   files.toString())

        val finishLatch = new CountDownLatch(1)
        val lock = new ReentrantLock()
        // connect to slave grpc server with id and create shuffling service async stub
        val slave_port:Int = idToEndpoint(id).split(":")(1).toInt
        val slave_ipaddress:String = idToEndpoint(id).split(":")(0)
        logger.info(slave_port.toString())
        logger.info(slave_ipaddress)
        val slaveChannel = ManagedChannelBuilder.forAddress(slave_ipaddress, slave_port).usePlaintext().asInstanceOf[ManagedChannelBuilder[_]].build
        val requestObserver = sendSortedFiles(lock, finishLatch,ShufflingGrpc.stub(slaveChannel))
        for (file <- files) {
          lock.lock()
          requestObserver.onNext(SendSortedFileRequest(file = ByteString.copyFrom(file)))
        }
        System.out.println("-------------we finished to send all data ----------------")
        requestObserver.onCompleted()
        finishLatch.await(1, TimeUnit.MINUTES)
       }    
    }
  }




  private class ShufflingImpl extends ShufflingGrpc.Shuffling{

    override def sendSortedFiles(responseObserver: StreamObserver[SendSortedFileResponse]): StreamObserver[SendSortedFileRequest] = {
      // check if current state is shuffling
      val requestObserver = new StreamObserver[SendSortedFileRequest] {
        override def onNext(value: SendSortedFileRequest) = {
          System.out.println(value.toByteArray.length)
          saveSortedFile(value.file.toByteArray(), ".")
          responseObserver.onNext(SendSortedFileResponse(ok = true))
        }
        override def onError(t: Throwable) = {
          logger.info("Error : " + t.toString())
        }
        override def onCompleted() = {
          responseObserver.onCompleted()
          System.out.println(" slave number " + id.toString + " has finish to receive all data")
        }
      }
      requestObserver
    }
  }

  def saveSortedFile(file: Array[Byte], parent_path: String) = { // later we will replace parent_path with Output directory
    System.out.println("slave number : " + id.toString() + "read file : " )
  
    val newFile =new File(parent_path ,id.toString +"-"+ index.toString)
    val fileWriter = new FileWriter(newFile)
    fileWriter.write(file.map(x=>x.toChar))
    fileWriter.flush()
    fileWriter.close()
    index +=1
    // save file to output directory with filename of {slaveId}-{index}
  }
}
