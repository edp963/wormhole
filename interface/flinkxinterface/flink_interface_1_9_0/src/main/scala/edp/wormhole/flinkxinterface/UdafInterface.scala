package edp.wormhole.flinkxinterface

import org.apache.flink.table.functions.AggregateFunction

abstract class UdafInterface[T, ACC] extends AggregateFunction[T, ACC]{

  /**
    * Creates and init the Accumulator for this [[AggregateFunction]].
    *
    * @return the accumulator with the initial value
    */
  def createAccumulator(): ACC // MANDATORY

  /**
    * Processes the input values and update the provided accumulator instance. The method
    * accumulate can be overloaded with different custom types and arguments. An AggregateFunction
    * requires at least one accumulate() method.
    *
    * param accumulator           the accumulator which contains the current aggregated results
    * param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  //def accumulate(accumulator: ACC, [user defined inputs]): Unit // MANDATORY

  /**
    * Called every time when an aggregation result should be materialized.
    * The returned value could be either an early and incomplete result
    * (periodically emitted as data arrive) or the final result of the
    * aggregation.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @return the aggregation result
    */
  def getValue(accumulator: ACC): T  // MANDATORY


  /**
    * Merges a group of accumulator instances into one accumulator instance. This function must be
    * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
    *
    * @param accumulator  the accumulator which will keep the merged aggregate results. It should
    *                     be noted that the accumulator may contain the previous aggregated
    *                     results. Therefore user should not replace or clean this instance in the
    *                     custom merge method.
    * @param its          an [[java.lang.Iterable]] pointed to a group of accumulators that will be
    *                     merged.
    */
  def merge(accumulator: ACC, its: java.lang.Iterable[ACC]): Unit = {}  // OPTIONAL

  /**
    * Retracts the input values from the accumulator instance. The current design assumes the
    * inputs are the values that have been previously accumulated. The method retract can be
    * overloaded with different custom types and arguments. This function must be implemented for
    * datastream bounded over aggregate.
    *
    * param accumulator           the accumulator which contains the current aggregated results
    * param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  //def retract(accumulator: ACC, [user defined inputs]): Unit // OPTIONAL



  /**
    * Resets the accumulator for this [[AggregateFunction]]. This function must be implemented for
    * dataset grouping aggregate.
    *
    * @param accumulator  the accumulator which needs to be reset
    */
  def resetAccumulator(accumulator: ACC): Unit = {}  // OPTIONAL

  /**
    * Returns true if this AggregateFunction can only be applied in an OVER window.
    *
    * return true if the AggregateFunction requires an OVER window, false otherwise.
    */
  //def requiresOver: Boolean = false // PRE-DEFINED

  /**
    * Returns the TypeInformation of the AggregateFunction's result.
    *
    * return The TypeInformation of the AggregateFunction's result or null if the result type
    *         should be automatically inferred.
    */
  //def getResultType: TypeInformation[T] = null // PRE-DEFINED

  /**
    * Returns the TypeInformation of the AggregateFunction's accumulator.
    *
    * return The TypeInformation of the AggregateFunction's accumulator or null if the
    *         accumulator type should be automatically inferred.
    */
  //def getAccumulatorType: TypeInformation[ACC] = null // PRE-DEFINED
}
