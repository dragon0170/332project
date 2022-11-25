package cs332.distributedsorting.master

import cs332.distributedsorting.slave.Slave
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

  test("3 slaves handshake with master") {
    val slaves = List[Slave](Slave("localhost", 50051), Slave("localhost", 50051), Slave("localhost", 50051))
    try {
      val handshakeFutures = for (slave <- slaves) yield Future {
        slave.handshake()
      }
      for (f <- handshakeFutures) Await.result(f, 5.seconds)
    } finally {
      for (slave <- slaves) slave.shutdown()
    }
  }
}