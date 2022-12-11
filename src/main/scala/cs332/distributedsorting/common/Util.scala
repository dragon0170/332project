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
			if(a(i)>b(i)) return 1
			if(a(i) < b(i)) return -1
		}
		return 0
  }
}

object KeyComparator extends Comparator[String]{
  override def compare (a:String, b : String):Int = {
    assert(a.length == b.length)
    val a_key : Array[Byte] = a.dropRight(90).map(c=>c.toByte).toArray
    val b_key : Array[Byte] = b.dropRight(90).map(c=>c.toByte).toArray
		for (i<- 0 to 9 ){
			if(a(i)>b(i)) return 1
			if(a(i) < b(i)) return -1
		}
		return 0
  }
}
