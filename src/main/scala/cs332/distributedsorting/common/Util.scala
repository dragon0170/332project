package cs332.distributedsorting.common

import java.net.{DatagramSocket, InetAddress}
import java.util.Comparator

object Util {
  def getMyIpAddress: String = {
    val socket = new DatagramSocket
    try {
      socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
      socket.getLocalAddress.getHostAddress
    } finally if (socket != null) socket.close()
  }
}

object KeyOrdering extends Ordering[Array[Byte]]{
  override def compare(a:Array[Byte], b: Array[Byte]):Int ={
		assert(a.length == b.length)
		for (i<- 0 to 9 ){
			if(a(i) > b(i)) return 1
			if(a(i) < b(i)) return -1
		}
		return 0
  }
}

object KeyComparator extends Comparator[String] {
  override def compare(a: String, b: String): Int = {
    assert(a.length == b.length)
//    if (a.take(10) == "%,9MoF'DDm") {
//      System.out.println(a)
//    }
//    if (b.take(10) == "%,9MoF'DDm") {
//      System.out.println(b)
//    }
//    System.out.println(s"a: ${a.take(10)}, b: ${b.take(10)}")
    val a_key: Array[Byte] = a.take(10).map(c => c.toByte).toArray
    val b_key: Array[Byte] = b.take(10).map(c => c.toByte).toArray
//    System.out.println(s"a: ${a_key.mkString("Array(", ", ", ")")}, b: ${b_key.mkString("Array(", ", ", ")")}")
//    if (a_key(0).==(32)) {
//    }
    for (i <- 0 to 9) {
      if (a_key(i) > b_key(i)) return 1
      if (a_key(i) < b_key(i)) return -1
    }
    return 0
  }
}
