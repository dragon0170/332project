package cs332.distributedsorting.common

import java.net.{DatagramSocket, InetAddress}
import scala.math.BigInt


object Util {
  def getMyIpAddress: String = {
    val socket = new DatagramSocket
    try {
      socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
      socket.getLocalAddress.getHostAddress
    } finally if (socket != null) socket.close()
  }
}
  /*
  def sub(a:Array[Byte], b : Array[Byte]):Array[Byte] = {
    assert(a.length == b.length)
    val aInt = BigInt(a)
    val bInt = BigInt(b)
    val tempInt = aInt - bInt
    var temp:Array[Byte] = BigInt(tempInt.intValue).toByteArray
    // we have to had padding because BigInt(1).toByteArray == Array(1), not (Array(0,0,0,1))
    while(temp.length < a.length){
      temp = Array(0.toByte) ++ temp
    }
    return temp
  }

  def add(a:Array[Byte], b : Array[Byte]):Array[Byte] = {
    assert(a.length == b.length)
    val aInt = BigInt(a)
    val bInt = BigInt(b)
    val tempInt = aInt + bInt 
    var temp: Array[Byte] = BigInt(tempInt.intValue).toByteArray
    while(temp.length < a.length){
      temp = Array(0.toByte) ++ temp
    }
    return temp
  }


  def mult(a:Int, b : Array[Byte]):Array[Byte] = {
    // a changer
    val aInt = BigInt(a)
    val bInt = BigInt(b)
    val tempInt = aInt * bInt 
    var temp: Array[Byte] = BigInt(tempInt.intValue).toByteArray
    while(temp.length < b.length){
      temp = Array(0.toByte) ++ temp
    }
    return temp
  }

  def div (a :Int, b : Array[Byte]) : Array[Byte] = {
    return mult(1/a,b)
  }
}
*/

  
  

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