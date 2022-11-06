package cs332.distributedsorting.master

import java.util.logging.Logger
import io.grpc.{Server, ServerBuilder}

import cs332.distributedsorting.example.{GreeterGrpc, HelloReply, HelloIpAdress}

import scala.concurrent.{ExecutionContext, Future}

object Master {
  private val logger = Logger.getLogger(classOf[Master].getName)

  def main(args: Array[String]): Unit = {
    val server = new Master(ExecutionContext.global)
    server.start()
    server.blockUntilShutdown()
  }

  private val port = 50062
}


class Master(executionContext: ExecutionContext) { self =>
  private[this] var server: Server = null
  private var clients: Map[String, String] = Map();
  


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

  private class GreeterImpl extends GreeterGrpc.Greeter {
    override def makeContact(req: HelloIpAdress) = {
      Master.logger.info("Hello from " + req.name)
      Master.logger.info("Get the Ip adress of " + req.name + "Ip adress is " + req.ipadress)
      clients = clients +(req.name -> req.ipadress)
      val reply = HelloReply(message = "Hello " + req.name)
      Future.successful(reply)

    }
  }

}
