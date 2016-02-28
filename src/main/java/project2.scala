import scala.math._
import scala.util.Random
import akka.actor.Actor
import akka.actor._
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props

//Actor cases
case class Structurize(i: Int, j: Int, k: Int, nodes: Array[Array[Array[ActorRef]]], index: Int, cuberoot: Int, topology: String)
case class pushSum(pushSum_s: Double,pushSum_w: Double, isSelf: Boolean, topology: String)
case class pushSumAll(pushSum_s: Double, pushSum_w: Double, isSelf: Boolean, topology: String, index: Int)
case class Gossip3D(selfFlag: Boolean)
case class GossipFull(NextIndex: Int, selfFlag: Boolean)
case class Close()

object Main extends App {
  var start: Long = 0
  var numNodes: Int = 0

  //Actor System, Actor references & index array declarations
  var allActors: Array[Int] = null
  var ActorsAllFull: Array[ActorRef] = null
  val InfoSys = ActorSystem("InformationSystem")

  //Check Command-line args
  if (args.length == 0 || args.length != 3) {
    println("Arguments are not proper.")
    InfoSys.shutdown()
  } else {
    //Read Command-line args
    numNodes = args(0).toInt
    val topology: String = args(1)
    val algorithm = args(2)

    //Compute nearest cuberoot
    val cuberoot = math.pow(numNodes, (0.33).toDouble).ceil.toInt

    var nodes_3d = Array.ofDim[ActorRef](0, 0, 0)

    //Mesh building for 3D based topologies
    if (topology.equalsIgnoreCase("3D") || topology.equalsIgnoreCase("imp3D")) {

      numNodes = cuberoot * cuberoot * cuberoot
      nodes_3d = Array.ofDim[ActorRef](cuberoot, cuberoot, cuberoot)

      //Creating 3D nodes
      var index: Int = 0

      for (i <- 0 to cuberoot - 1) {
        for (j <- 0 to cuberoot - 1) {
          for (k <- 0 to cuberoot - 1) {
            nodes_3d(i)(j)(k) = InfoSys.actorOf(Props[Node])
          }
        }
      }

      for (i <- 0 to cuberoot - 1) {
        for (j <- 0 to cuberoot - 1) {
          for (k <- 0 to cuberoot - 1) {
            nodes_3d(i)(j)(k) ! Structurize(i, j, k, nodes_3d, index, cuberoot, topology)
            index = index + 1
          }
        }
      }
    }

    //Mesh building for Line topology
    if (topology.equalsIgnoreCase("line")) {

      //Creating nodes
      var index: Int = 0
      nodes_3d = Array.ofDim[ActorRef](1, 1, numNodes)

      for (k <- 0 to numNodes - 1) {
        nodes_3d(0)(0)(k) = InfoSys.actorOf(Props[Node])
      }

      for (k <- 0 to numNodes - 1) {
        nodes_3d(0)(0)(k) ! Structurize(0, 0, k, nodes_3d, index, numNodes, topology)
        index = index + 1
      }
    }

    //Mesh building for Line topology
    if (topology.equalsIgnoreCase("full")) {
      ActorsAllFull = new Array[ActorRef](numNodes)
      for (i <- 0 to numNodes - 1) {
        ActorsAllFull(i) = InfoSys.actorOf(Props[Node])
      }
    }

    //Mark all nodes as "UP"
    allActors = new Array[Int](numNodes)
    for (i <- 0 to allActors.length - 1) {
      allActors(i) = 1
    }

    //Initiate Gossip as per topology
    if (algorithm.equalsIgnoreCase("gossip")) {

      if (topology.equalsIgnoreCase("3D") || topology.equalsIgnoreCase("imp3D")) {
        start = System.currentTimeMillis()
        nodes_3d(Random.nextInt(cuberoot))(Random.nextInt(cuberoot))(Random.nextInt(cuberoot)) ! Gossip3D(false)
      }

      if (topology.equalsIgnoreCase("line")) {
        start = System.currentTimeMillis()
        nodes_3d(0)(0)(Random.nextInt(numNodes)) ! Gossip3D(false)
      }

      if (topology.equalsIgnoreCase("full")) {
        start = System.currentTimeMillis()
        var randomfullnext: Int = Random.nextInt(ActorsAllFull.length)
        ActorsAllFull(randomfullnext) ! GossipFull(randomfullnext, false)
      }

    }

    //Initiate Push-Sum as per topology
    if (algorithm.equalsIgnoreCase("push-sum")) {

      if (topology.equalsIgnoreCase("3D") || topology.equalsIgnoreCase("imp3D")) {
        start = System.currentTimeMillis()
        nodes_3d(Random.nextInt(cuberoot))(Random.nextInt(cuberoot))(Random.nextInt(cuberoot)) ! pushSum(0, 1, true, topology)
      }

      if (topology.equalsIgnoreCase("line")) {
        start = System.currentTimeMillis()
        nodes_3d(0)(0)(Random.nextInt(numNodes)) ! pushSum(0, 1, true, topology)
      }

      if (topology.equalsIgnoreCase("full")) {
        start = System.currentTimeMillis()
        var randomNode: Int = Random.nextInt(ActorsAllFull.length)
        ActorsAllFull(randomNode) ! pushSumAll(0, 1, true, topology, randomNode)
      }

    }
  }
}

class Node() extends Actor {

  //Initialize variables
  var thisNode: ActorRef = null
  var neighbor = Array.ofDim[ActorRef](7)
  var neighborijk = Array.ofDim[String](7)
  var size: Int = 0
  var thispushSum_s: Double = 0.0
  var thispushSum_w: Double = 0.0
  var estimateRatio: Double = 0.0
  var estimateRatioCount: Int = 0
  var estimateRatioDifferenceThreshold: Double = math.pow(10, -10)
  var nodeIndex: Int = 0
  var GossipCount: Int = 0
  var ijk_index: Int = 0
  var neighbor_index: Int = 0
  var end: Long = 0

  def receive = {
    //Identify neighbors (Line, 3D, imp3D)
    case Structurize(i: Int, j: Int, k: Int, nodes: Array[Array[Array[ActorRef]]], index: Int, cuberoot: Int, topology: String) => {

      nodeIndex = index
      thispushSum_s = index.toDouble
      thispushSum_w = 1.0

      thisNode = nodes(i)(j)(k)
      size = cuberoot

      if (!topology.equalsIgnoreCase("line")) {

        //left
        if (i != 0) {
          neighbor(neighbor_index) = nodes(i - 1)(j)(k)
          neighbor_index += 1
          neighborijk(ijk_index) = (i - 1).toString() + "|" + j.toString() + "|" + k.toString()
          ijk_index += 1
        }
        //right
        if (i != cuberoot - 1) {
          neighbor(neighbor_index) = nodes(i + 1)(j)(k)
          neighbor_index += 1
          neighborijk(ijk_index) = (i + 1).toString() + "|" + j.toString() + "|" + k.toString()
          ijk_index += 1
        }
        //back
        if (j != 0) {
          neighbor(neighbor_index) = nodes(i)(j - 1)(k)
          neighbor_index += 1
          neighborijk(ijk_index) = i.toString() + "|" + (j - 1).toString() + "|" + k.toString()
          ijk_index += 1
        }
        //front
        if (j != cuberoot - 1) {
          neighbor(neighbor_index) = nodes(i)(j + 1)(k)
          neighbor_index += 1
          neighborijk(ijk_index) = i.toString() + "|" + (j + 1).toString() + "|" + k.toString()
          ijk_index += 1
        }
      }

      //bottom
      if (k != 0) {
        neighbor(neighbor_index) = nodes(i)(j)(k - 1)
        neighbor_index += 1
        neighborijk(ijk_index) = i.toString() + "|" + j.toString() + "|" + (k - 1).toString()
        ijk_index += 1
      }
      //top
      if (k != cuberoot - 1) {
        neighbor(neighbor_index) = nodes(i)(j)(k + 1)
        neighbor_index += 1
        neighborijk(ijk_index) = i.toString() + "|" + j.toString() + "|" + (k + 1).toString()
        ijk_index += 1
      }

      //imp 3D - random neighbor add
      if (topology.equalsIgnoreCase("imp3d")) {
        var randomi: Int = Random.nextInt(cuberoot)
        var randomj: Int = Random.nextInt(cuberoot)
        var randomk: Int = Random.nextInt(cuberoot)
        var randomNeigh: ActorRef = nodes(randomi)(randomj)(randomk)
        while ((randomNeigh.equals(self)) || (neighbor contains randomNeigh)) {
          randomi = Random.nextInt(cuberoot)
          randomj = Random.nextInt(cuberoot)
          randomk = Random.nextInt(cuberoot)
          randomNeigh = nodes(randomi)(randomj)(randomk)
        }

        neighbor(neighbor_index) = randomNeigh
        neighbor_index += 1
        neighborijk(ijk_index) = randomi.toString() + "|" + randomj.toString() + "|" + randomk.toString()
        ijk_index += 1
      }
    }

    case Gossip3D(selfFlag: Boolean) => {

      //Increment counter
      if (GossipCount < 10) {
        if (!selfFlag) {
          GossipCount += 1

        }

        //Forward to random neighbor
        var randomNeighbor: ActorRef = neighbor(fetchRandomNeighbor("3D"))
        randomNeighbor ! Gossip3D(false)

      }

      //Ticker
      if (GossipCount < 9) { self ! Gossip3D(true) }

      //Terminating condition
      if (GossipCount == 10) {
        self ! Close
      }

    }
    case GossipFull(nextIndex: Int, selfFlag: Boolean) => {
      nodeIndex = nextIndex

      //Check counter & forward to random neighbor
      if (GossipCount < 10) {
        var targetIndex: Int = fetchRandomNeighbor("full")
        var randomNeighbor: ActorRef = Main.ActorsAllFull(targetIndex)

        if (Main.allActors(targetIndex) == 1) {
          if (!selfFlag) {
            GossipCount += 1

          }

          randomNeighbor ! GossipFull(targetIndex, false)
        }
      }

      //Ticker
      if (GossipCount < 9) {
        self ! GossipFull(nodeIndex, true)
      }

      //Terminating condition
      if (GossipCount == 10) {
        self ! Close
      }
    }

    case pushSum(s: Double, w: Double, isSelf: Boolean, topology: String) => {

      //Calculate Push Sum
      calcPushSum(s, w, isSelf)

      //Forward to random neighbor
      var randomNeighbor: ActorRef = neighbor(fetchRandomNeighbor(topology))
      randomNeighbor ! pushSum(thispushSum_s, thispushSum_w, false, topology)

      //Ticker
      self ! pushSum(0, 0, true, topology)
    }

    case pushSumAll(s: Double, w: Double, isSelf, topology: String, index: Int) => {

      nodeIndex = index

      //Calculate Push Sum
      calcPushSum(s, w, isSelf)

      //Forward to  random neighbor
      var targetIndex: Int = fetchRandomNeighbor(topology)
      var randomNeighbor: ActorRef = Main.ActorsAllFull(targetIndex)
      randomNeighbor ! pushSumAll(thispushSum_s, thispushSum_w, false, topology, targetIndex)

      //Ticker
      self ! pushSumAll(0, 0, true, topology, nodeIndex)
    }

    case Close => {

      //Mark node as "DOWN"
      Main.allActors(nodeIndex) = 0

      //Calculate convergence time & progress
      end = System.currentTimeMillis()
      var convergence_time = (end - Main.start).toDouble
      var convergence_reached = ((Main.allActors.deep.mkString.count(_ == '0').toFloat/Main.numNodes)*100).toDouble
      printf("Convergence time for Actor %s = %s | Convergence reached: %s %%\n", nodeIndex, convergence_time, convergence_reached)
      context.stop(self)
    }
  }

  def fetchRandomNeighbor(topology: String): Int = {

    var keepSearching: Boolean = true

    //Fetch random neighbor for Topologies (Line, 3D, imp3D)
    if (topology.equalsIgnoreCase("3d") || topology.equalsIgnoreCase("imp3d") || topology.equalsIgnoreCase("line")) {
      while (keepSearching) {
        var RandomNode = Random.nextInt(neighbor_index)
        var Randijk = neighborijk(RandomNode)
        var Neighbori = Randijk.split("|")(0).toInt
        var Neighborj = Randijk.split("|")(2).toInt
        var Neighbork = Randijk.split("|")(4).toInt
        var OrigIndex = (((Neighbori * size) + Neighborj) * size) + Neighbork
        if (Main.allActors(OrigIndex) == 1) {
          keepSearching = false

          return RandomNode
        }
      }
    }

    //Fetch random neighbor for Full Network Topology
    if (topology.equalsIgnoreCase("full")) {
      var RandomNode: Int = Random.nextInt(Main.ActorsAllFull.length)
      while (keepSearching) {
        RandomNode = Random.nextInt(Main.ActorsAllFull.length)
        if (Main.allActors(RandomNode) == 1) {
          if (RandomNode != nodeIndex) {
            keepSearching = false

            return RandomNode
          }
        }
      }
    }
    return -1
  }

  def calcPushSum(s: Double, w: Double, isSelf: Boolean) = {

    //receive message and add to self
    thispushSum_s += s
    thispushSum_w += w

    //calculate sum estimate
    var tempestimateRatio: Double = thispushSum_s / thispushSum_w

    //Modify counter
    if (!isSelf) {
      if (abs(tempestimateRatio - estimateRatio) < estimateRatioDifferenceThreshold) {
        estimateRatioCount += 1
      } else {
        estimateRatioCount = 0
      }
    }

    //Update Sum Estimate
    estimateRatio = tempestimateRatio

    //Terminating condition
    if (estimateRatioCount == 3) {

      self ! Close
    }

    //keep half and send half
    thispushSum_s = thispushSum_s / 2
    thispushSum_w = thispushSum_w / 2
  }
}
