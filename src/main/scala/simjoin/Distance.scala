package simjoin

object Distance {
  def distance(first: String, second: String): Int = {
    val matrix  = Array.ofDim[Int](first.length()+1, second.length()+1)
    for(i <- 1 to first.length()) {
      matrix(i)(0) = i
    }
    for(j <- 1 to second.length()) {
      matrix(0)(j) = j
    }

    for(i <- 1 to first.length()) {
      for (j <- 1 to second.length()) {
        if (first.charAt(i-1) != second.charAt(j-1)) {
          val k = math.min(
            math.min(
              matrix(i)(j - 1),
              matrix(i - 1)(j)),
            matrix(i - 1)(j - 1)
          )
          matrix(i)(j) = k + 1
        }
        else matrix(i)(j) = matrix(i-1)(j-1)
      }
    }

    matrix(first.length())(second.length())

  }
}
