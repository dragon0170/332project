package cs332.distributedsorting.common

import java.net.{DatagramSocket, InetAddress}

object Util {
  def getMyIpAddress: String = {
    val socket = new DatagramSocket
    try {
      socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
      socket.getLocalAddress.getHostAddress
    } finally if (socket != null) socket.close()
  }

  def compareKeys(a:String, b: String):Boolean ={
		assert(a.length() == b.length())
		for (i<- 0 to 9 ){
			if(a(i)>b(i)) return true
			if(a(i) < b(i)) return false
		}
		return true
  }
}