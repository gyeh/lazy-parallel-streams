import annotation.tailrec
import util.Random
import scala.concurrent.{ future, Future, Await, Promise }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.Mutable

/**
 *
 * Requires Scala 2.10 (because of the shiny new Akka Futures).
 * 
 * Parallel Collections were explicitly avoided because it would make certain tasks too trivial.
 *
 */

trait LazyStream[A] {
  def popNext: Option[A]
  def popN(n: Int): Seq[A] = (1 to n).map(_ => popNext).flatten
}

/**
 * *******************************************************************************
 * (Stream Implementations)
 * ******************************************************************************
 */

class RandomStream extends LazyStream[Int] {
  def popNext: Option[Int] = Some(Random.nextInt())
}

class PrimeStream extends LazyStream[Int] {
  def sieve(s: Stream[Int]): Stream[Int] = {
    Stream.cons(s.head, sieve(s.tail.filter(i => (i % s.head) != 0)))
  }

  private[this] var primeStream = sieve(Stream.from(2))

  def popNext: Option[Int] = {
    val retVal = primeStream.head
    primeStream = primeStream.tail
    Some(retVal)
  }
}

class PrimeFactorsStream(private[this] var n: Int) extends LazyStream[Int] {
  private[this] val primeStream = new PrimeStream
  private[this] var currentPrime = primeStream.popNext.get

  def popNext: Option[Int] = {
    if (currentPrime > n) None
    else if (n % currentPrime != 0) {
      currentPrime = primeStream.popNext.get
      popNext
    } else {
      n = n / currentPrime
      Some(currentPrime)
    }
  }
}

/**
 * *******************************************************************************
 * (Stream Combinators)
 *
 * Parallelized popN() is implemented for map(), filter(), zipWith(), prefixReduce().  *It is assumed that fn() is
 * an expensive operation, such that the marginal benefits of calling fn() in a separate parallel call exceeds the
 * marginal costs of supporting the parallel call (ie. threads aren't cheap).*
 *
 * In this case, each fn() will be executed in parallel (in this case, by a Future that is backed by a Thread pool).
 * ******************************************************************************
 */

object LazyStream {

  val defaultWaitTime = 30 seconds

  def map[A, B](fn: A => B, stream: LazyStream[A]): LazyStream[B] = {
    new LazyStream[B] {
      def popNext: Option[B] = stream.popNext.map(i => fn(i))

      override def popN(n: Int): Seq[B] = {
        val values = stream.popN(n)

        // a new future is created to async process fn().
        val futures = values.map { value =>
          future { fn(value) }
        }

        // blocking call to await all results
        Await.result(Future.sequence(futures), defaultWaitTime)
      }
    }
  }

  def filter[A](fn: A => Boolean, stream: LazyStream[A]): LazyStream[A] = {
    new LazyStream[A] {
      def popNext: Option[A] = {
        stream.popNext match {
          case None => None
          case Some(value) => if (fn(value)) Some(value) else popNext
        }
      }

      override def popN(n: Int): Seq[A] = {
        val values = stream.popN(n)

        // filter out values that do not match predicate
        val futures = values.map { value =>
          future { (fn(value), value) }
        }

        // blocking call to await all results
        Await.result(Future.sequence(futures), defaultWaitTime).filter(_._1).map(_._2)
      }
    }

  }

  def zipWith[A, B, C](fn: (A, B) => C, stream1: LazyStream[A], stream2: LazyStream[B]): LazyStream[C] = {
    new LazyStream[C] {
      def popNext: Option[C] = for (left <- stream1.popNext; right <- stream2.popNext) yield fn(left, right)

      override def popN(n: Int): Seq[C] = {
        val values1 = stream1.popN(n)
        val values2 = stream2.popN(n)

        val futures = values1.zip(values2).map {
          case (value1, value2) =>
            future { fn(value1, value2) }
        }

        // blocking call to await all results
        Await.result(Future.sequence(futures), defaultWaitTime)
      }
    }

  }

  /*
   * Returns the current accummulative state so far.  Assuming that the accumlated result type is the same as input
   * (unlike a traditional fold).
   */
  def prefixReduce[A](fn: (A, A) => A, stream: LazyStream[A], init: A): LazyStream[A] = {

    new LazyStream[A] {
      private[this] var accum: A = init

      def popNext: Option[A] = {
        stream.popNext.map { i =>
          accum = fn(i, accum)
          accum
        }
      }

      /*
       * Implementation based on parallel prefix-sum algorithm as described here:
       * http://www.cs.washington.edu/education/courses/cse332/10sp/lectures/lecture20.pdf
       *
       * Implementation uses explicit trees to handle both bottom-up and top-down passes.  While it is much 
       * less efficient compared to the traditional approach of using an in-place array, it is much easier to maintain 
       * and reason about by using a tree, especially within a multi-threaded environment because state is kept immutable.
       * 
       */
      override def popN(n: Int): Seq[A] = {
        val values = stream.popN(n)

        val tree = upSweep(values, closestPowerOfTwo(values.length, 1))
        init +: downSweep(tree, init)
      }

      import scala.math.pow
      def closestPowerOfTwo(n: Int, i: Int): Int = {
        if (n > pow(2, i)) closestPowerOfTwo(n, i + 1)
        else pow(2, i).toInt
      }

      /*
       * Primary data structure to repesent tree nodes for parallel prefix reduce problem
       *
       * value: the reduced value of all items within "range" or children nodes; as calculated by fn()
       */
      abstract class Tree[+T]
      case class Node[T](value: T, left: Tree[T], right: Tree[T]) extends Tree[T]
      case object End extends Tree[Nothing]

      /*
       * Building up a complete binary tree to represent accumulated reduction.
       * 
       * Size of tree is based on number of leaves (powerOfTwo).  For the sake of
       * efficiency during the downSweep phase and tree representation; nodes that
       * represent empty values are "trimmed" early and are respresented as "End".
       * 
       * Using futures to emulate Java 7's Fork/Join framework
       */
      private[this] def upSweep(values: Seq[A], powerOfTwo: Int): Tree[A] = {
        if (values.length == 0) End
        else if (values.length == 1) Node(values.head, End, End)
        else {
          val (leftValue, rightValue) = values.splitAt(powerOfTwo / 2)
          val futureChildren = future { upSweep(leftValue, powerOfTwo / 2) } zip
            									 future { upSweep(rightValue, powerOfTwo / 2) }

          val futureNode = futureChildren.map {
            case (leftNode, rightNode) =>
              (leftNode, rightNode) match {
                case (End, End) => End
                case (leftNode: Node[A], End) => Node(leftNode.value, leftNode, rightNode)
                case (End, rightNode: Node[A]) => Node(rightNode.value, leftNode, rightNode)
                case (leftNode: Node[A], rightNode: Node[A]) => Node(fn(leftNode.value, rightNode.value), leftNode, rightNode)
              }
          }

          Await.result(futureNode, defaultWaitTime)
        }
      }

      /*
       * Traverse down tree to calculate prefix reductions.
       * 
       * Using futures to emulate Java 7's Fork/Join framework
       */
      private[this] def downSweep(node: Tree[A], passDownValue: A): Seq[A] = {
        node match {
          case End => Seq()
          case Node(value, End, End) => Seq(fn(value, passDownValue))
          case Node(_, left: Node[A], End) => {
            val futureSeq = future { downSweep(left, passDownValue) }
            Await.result(futureSeq, defaultWaitTime)
          }
          case Node(_, left: Node[A], right: Node[A]) => {
            val futures = future { downSweep(left, passDownValue) } zip
            						  future { downSweep(right, fn(passDownValue, left.value)) }

            val futureSeq = futures.map {
              case (leftSeq, rightSeq) =>
                leftSeq ++ rightSeq
            }

            Await.result(futureSeq, defaultWaitTime)
          }
        }
      }

    }
  }

}


object Test extends App {

  // Very coarse benchmarks.  Note that timings are run on a 2.4Ghz dual-core Macbook Pro.
  def timer[A](a: => A) = {
    val now = System.nanoTime
    val result = a
    val micros = (System.nanoTime - now) / 1000
    println("%d microseconds".format(micros))
    result
  }
  
  /* Streams */
  val randomStream = new RandomStream
  val primeStream = new PrimeStream
  val primeFactor = new PrimeFactorsStream(1000)

  println(randomStream.popNext) // => Some(378525576)
  println(randomStream.popN(8)) // => Vector(1859491545, 2069652625, 1643044939, 1413750587, 287508000, 1263909646, -1041697252, 2056937152)
  
  println(primeStream.popNext) // => Some(2)
  println(primeStream.popN(8)) // => Vector(3, 5, 7, 11, 13, 17, 19, 23)

  println(primeFactor.popN(3)) // => Vector(2, 2, 2)
  println(primeFactor.popNext) // => Some(5)
  println(primeFactor.popN(100)) // => Vector(5, 5)
  
  println(LazyStream.zipWith((i: Int, j:Int) => i + j, new PrimeStream, new PrimeFactorsStream(1000)).popN(100))
  // => Vector(4, 5, 7, 12, 16, 18)
  
  
  
  /* Map */
  val mapStreamTimed = LazyStream.map((i: Int) => { Thread.sleep(200); i + 1 }, new PrimeStream)
  
  // => parallel map: 10103547 microseconds
  val pmapResult = timer { mapStreamTimed.popN(100) }
  
  // => sequential map: 20112578 microseconds
  val smapResult = timer { (new PrimeStream).popN(100).map { i => { Thread.sleep(200); i + 1 } } }
  
  assert(pmapResult == smapResult)
  

  
  
  /* Filter */
  val filterStreamTimed = LazyStream.filter((i: Int) => { Thread.sleep(200); i == 2 }, new PrimeFactorsStream(1000))
  
  // => parallel filter: 611137 microseconds
  val pfilterResult = timer { filterStreamTimed.popN(100) }
  
  // => sequential filter 1205717 microseconds
  val sfilterResult = timer { (new PrimeFactorsStream(1000)).popN(100).filter { i => { Thread.sleep(200); i == 2 } } }
  
  assert(pfilterResult == sfilterResult)

  
  
  /* Reduce */
  val reduceStream = LazyStream.prefixReduce((i: Int, j: Int) => { Thread.sleep(200); i + j }, new PrimeStream, 0)
  
  // => parallel reduce: 16578269 microseconds 
  val preduceResult = timer { reduceStream.popN(100) }
  
  // => sequential reduce: 20099352 microseconds
  val sreduceResult = timer { (new PrimeStream).popN(100).scan(0) { (a, i) => { Thread.sleep(200); a + i } } }
  
  assert(preduceResult == sreduceResult)
  
}


