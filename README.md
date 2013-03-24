# LazyStreams

This is a experimental combinator library that supports parallel
computations.
Underlying implementation utilizes Akka Futures, which was recently
released for Scala 2.10.

## Details

Specific parallel combinator support is provided for map(), filter(), zipWith()
and prefixReduce().

prefixReduce() implementation is based on the parallel prefix-sum
algorithm as described here: http://www.cs.washington.edu/education/courses/cse332/10sp/lectures/lecture20.pdf.

Implementation uses explicit trees to handle both bottom-up and top-down passes.  While it is much less efficient compared to the traditional approach of using an in-place array, it is much easier to maintain and reason about by using a tree, especially within a multi-threaded environment because state is kept immutable.

Specific logic flow emulates Java 7's Fork/Join Framework.
