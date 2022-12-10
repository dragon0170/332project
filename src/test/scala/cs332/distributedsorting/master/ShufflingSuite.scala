package cs332.distributedsorting.master

import cs332.distributedsorting.slave.Slave
import cs332.distributedsorting.master.Master
import org.scalatest._
import org.scalatest.funsuite._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}
import org.checkerframework.checker.units.qual.s
import cs332.distributedsorting.common.KeyRange

class SortingSuite extends AnyFunSuite {
  override def withFixture(test: NoArgTest): Outcome = {
    val vector : Vector[SlaveClient] = Vector(new SlaveClient(1, "localhost", 0), new SlaveClient(2,"localhost", 0))
    val master = new Master(ExecutionContext.global, 2, vector)
    master.start()
    try test()
    finally {
      master.stop()
    }
  }


  test("SetSlaverPort  with 2 slaves") {
    val result : Map[Int, String] = Map((1->"localhost:5191"), (2->"localhost:5192"))
    val idToKeyRangeForTest = Map((1->(new KeyRange(Array(0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte,0.toByte), Array(40.toByte,60.toByte,110.toByte,110.toByte,110.toByte,110.toByte,110.toByte,110.toByte,110.toByte,110.toByte)))),
    2->(new KeyRange(Array(40.toByte,60.toByte,110.toByte,110.toByte,110.toByte,110.toByte,110.toByte,110.toByte,110.toByte,111.toByte), Array(255.toByte,255.toByte,255.toByte,255.toByte,255.toByte,255.toByte,255.toByte,255.toByte,255.toByte,255.toByte))))
    val slaves = List[Slave](Slave("localhost", 5190, "test.txt", 1, 5191, idToKeyRangeForTest), Slave("localhost", 5190,"test2.txt", 2, 5192, idToKeyRangeForTest))
    for (slave <-slaves )
        slave.startGrpcServer()
    
    try {
      val SetSlavePortFutures = for (slave <- slaves) yield Future {
        slave.setSlaveServerPort()
      }
      for (f <- SetSlavePortFutures) Await.result(f, 5.seconds)
      assert(slaves(0).idToEndpoint ==result)
      assert(slaves(1).idToEndpoint == result)

    } finally {
      for (slave <- slaves) {
        slave.stopServer()
        slave.shutdown()
      }
    }
  }
}
