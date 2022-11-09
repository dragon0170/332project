package cs332.distributedsorting.master

import java.util.logging.Logger
import io.grpc.{Server, ServerBuilder}

import cs332.distributedsorting.example.{GreeterGrpc, HelloReply, HelloRequest}

import scala.concurrent.{ExecutionContext, Future}

object Master {
  private val logger = Logger.getLogger(classOf[Master].getName)

  def main(args: Array[String]): Unit = {
    val server = new Master(ExecutionContext.global)
    try {
      val slave_num = args.toInt
      val localhost: InetAddress = InetAddress.getLocalHost
      val localIpAddress: String = localhost.getHostAddress
      server.start()
      server.printMasterIP(slave_num, localIpAddress)
    } finally {
      server.blockUntilShutdown()
    }
  }

  private val port = 50062
}

class Master(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null

  private def start(): Unit = {
    server = ServerBuilder.forPort(Master.port).addService(GreeterGrpc.bindService(new GreeterImpl, executionContext)).build.start
    Master.logger.info("Server started, listening on " + Master.port)
    sys.addShutdownHook {
      System.err.println("*** shutting down gRPC server since JVM is shutting down")
      self.stop()
      System.err.println("*** server shut down")
    }
  }

  private def stop(): Unit = {
    if (server != null) {
      server.shutdown()
    }
  }

  private def blockUntilShutdown(): Unit = {
    if (server != null) {
      server.awaitTermination()
    }
  }

  private def printMasterIP(slave_num: Int, ip: String): Unit = {
    if (server != null) {
      System.out.println("Master IP: " ip + Master.port)
      for{
        i <- 1 to slave_num
        client <- clients
      }yield System.out.println("Worker IP #" + i + ": " client._2)
    }
  }
  
  private class GreeterImpl extends GreeterGrpc.Greeter {
    override def sayHello(req: HelloRequest) = {
      Master.logger.info("Hello from " + req.name)
      val reply = HelloReply(message = "Hello " + req.name)
      Future.successful(reply)
    }
  }

}
