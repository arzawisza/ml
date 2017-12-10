package com.azawisza.logs

object Filter {
  def isLfiThreat(lfi: Set[String], e: String) :Boolean= {
    for (l <- lfi) {
      if (e.contains(l)) {

        return true
      }
    }
    false
  }
}
