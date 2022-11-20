package cs332.distributedsorting.common

import java.net.{DatagramSocket, InetAddress}
import com.google.code.externalsorting.ExternalSort  //to enable externalsortinjava
import java.io.File
import scala.math.BigInt


object Util {
  def getMyIpAddress: String = {
    val socket = new DatagramSocket
    try {
      socket.connect(InetAddress.getByName("8.8.8.8"), 10002)
      socket.getLocalAddress.getHostAddress
    } finally if (socket != null) socket.close()
  }
  
  def getListOfFiles(dir: String): List[File] = {
    def go(dir: File): List[File] = dir match {
      case d if d.exists && d.isDirectory && d.canRead=>
        val files = d.listFiles.filter(a => a.canRead && a.isFile).toList
        val dirs  = dir.listFiles.filter(_.isDirectory).toList
        files ::: dirs.foldLeft(List.empty[File])(_ ::: go(_))
      case _ => List.empty[File]
    }
    go(new File(dir))
  }
  
  def externalSort(dirList:List[String], outputDir:String): Unit = {
    val sortedInputFileList:List[File] = for(inputFileDir <- dirList){
      for(inputFile <- getListOfFiles(inputFileDir)){
        ExternalSort.sort​(inputFile, inputFile)
      }
    }yield inputFile
    
    ExternalSort.mergeSortedFiles(sortedInputFileList:List[File], new File(outputDir + "whole"));
  }
  
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