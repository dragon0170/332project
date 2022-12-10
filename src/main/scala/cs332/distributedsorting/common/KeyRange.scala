package cs332.distributedsorting.common

class KeyRange(
    val lowerbound : Array[Byte],
    val upperbound : Array[Byte]) {
  


  def inRange(key : Array[Byte]):Boolean={
    if (key.length != 10) 
      false
    else if (KeyOrdering.compare(key, upperbound) > 0)
      return false
    else if (KeyOrdering.compare(key, lowerbound) <0)
      return false 
    return true

  }
}

