package V3

object tmp {

}

object Main { // Recursive Function to print path of given
  // vertex u from source vertex v
  def printPath(path: Array[Array[Int]], v: Int, u: Int): Unit = {
    if (path(v)(u) == v) return
    printPath(path, v, path(v)(u))
    System.out.print(path(v)(u) + " ")
  }

  // Function to print the shortest cost with path
  // information between all pairs of vertices
  def printSolution(cost: Array[Array[Int]], path: Array[Array[Int]], N: Int): Unit = {
    var v = 0
    while ( {
      v < N
    }) {
      var u = 0
      while ( {
        u < N
      }) {
        if (u != v && path(v)(u) != -1) {
          System.out.print("Shortest Path from vertex " + v + " to vertex " + u + " is (" + v + " ")
          printPath(path, v, u)
          System.out.println(u + ")")
        }

        {
          u += 1; u - 1
        }
      }

      {
        v += 1; v - 1
      }
    }
  }

  // Function to run Floyd-Warshell algorithm
  def FloydWarshell(adjMatrix: Array[Array[Int]], N: Int): Unit = { // cost[] and parent[] stores shortest-path
    // (shortest-cost/shortest route) information
    val cost = new Array[Array[Int]](N)
    val path = new Array[Array[Int]](N)
    // initialize cost[] and parent[]
    var v = 0
    while ( {
      v < N
    }) {
      var u = 0
      while ( {
        u < N
      }) { // initally cost would be same as weight
        // of the edge
        cost(v)(u) = adjMatrix(v)(u)
        if (v == u) path(v)(u) = 0
        else if (cost(v)(u) != Integer.MAX_VALUE) path(v)(u) = v
        else path(v)(u) = -1

        {
          u += 1; u - 1
        }
      }

      {
        v += 1; v - 1
      }
    }
    // run Floyd-Warshell
    var k = 0
    while ( {
      k < N
    }) {
      var v = 0
      while ( {
        v < N
      }) {
        var u = 0
        while ( {
          u < N
        }) { // If vertex k is on the shortest path from v to u,
          // then update the value of cost[v][u], path[v][u]
          if (cost(v)(k) != Integer.MAX_VALUE && cost(k)(u) != Integer.MAX_VALUE && (cost(v)(k) + cost(k)(u) < cost(v)(u))) {
            cost(v)(u) = cost(v)(k) + cost(k)(u)
            path(v)(u) = path(k)(u)
          }

          {
            u += 1; u - 1
          }
        }
        // if diagonal elements become negative, the
        // graph contains a negative weight cycle
        if (cost(v)(v) < 0) {
          System.out.println("Negative Weight Cycle Found!!")
          return
        }
        {
          v += 1; v - 1
        }
      }

      {
        k += 1; k - 1
      }
    }
    // Print the shortest path between all pairs of vertices
    printSolution(cost, path, N)
  }

  def main(args: Array[String]): Unit = { // Number of vertices in the adjMatrix
    val N = 6
    val i = Integer.MAX_VALUE
    // given adjacency representation of matrix
    val adjMatrix = Array[Array[Int]](
      Array(i,50,10,i,45,i),
      Array(i,i,15,i,i,i),
      Array(20,i,i,15,i,i),
      Array(i,20,i,i,35,i),
      Array(i,i,i,30,i,i),
      Array(i,i,i,3,i,i)
    )

    // Run Floyd Warshell algorithm
    FloydWarshell(adjMatrix, N)
  }
}