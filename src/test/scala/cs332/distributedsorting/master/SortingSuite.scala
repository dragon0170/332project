package cs332.distributedsorting.master

import cs332.distributedsorting.slave.Slave
import cs332.distributedsorting.master.Master
import org.scalatest._
import org.scalatest.funsuite._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

class SortingSuite extends AnyFunSuite {
  override def withFixture(test: NoArgTest): Outcome = {
    val master = new Master(ExecutionContext.global, 3)
    master.start()
    try test()
    finally {
      master.stop()
    }
  }


  test("3 slaves SendData to master"){
    val slaves = List[Slave](Slave("localhost", 5133,"1"), Slave("localhost", 5133,"2"), Slave("localhost",5133,"3"))
    try{
      val handshakeFuture = for (slave <- slaves) yield Future{
        slave.handshake()
      }
      for(f<-handshakeFuture)
        Await.result(f, 5.seconds)

      val partitionFuture = for (slave <- slaves) yield Future{
      // give the path of the file as parameter
        if (slave.name == "1")
          slave.sendData("/Users/mathisayma_1/SoftwareDesignMethods/test.txt")
        else if(slave.name == "2")
          slave.sendData("/Users/mathisayma_1/SoftwareDesignMethods/test2.txt")
        else 
          slave.sendData("/Users/mathisayma_1/SoftwareDesignMethods/test3.txt")
      }
      for (f<-partitionFuture)
        Await.result(f, 5.seconds)
    }finally{
     for (slave <- slaves) slave.shutdown()
    }
  }
}
