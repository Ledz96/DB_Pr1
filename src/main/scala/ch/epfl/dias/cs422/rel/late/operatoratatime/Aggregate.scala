package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  var data: IndexedSeq[Tuple] = IndexedSeq[Tuple]()

  override def execute(): IndexedSeq[Column] = {
    var vids = IndexedSeq[Column]()
    val oldVids = input.execute()
    //Empty table case
    if (oldVids.isEmpty) {
      var tuple: Tuple = IndexedSeq()
      for (call <- aggCalls) {
        tuple :+= call.emptyValue
      }

      data :+= tuple
      vids :+= IndexedSeq(0L)
      return vids
    }

    //Building the table
    var table = IndexedSeq[Tuple]()
    for (oldVid <- oldVids) {
      table :+= input.evaluators().apply(oldVid)
    }

    //Empty groupset
    if (groupSet.isEmpty) {
      var tuple: Tuple = IndexedSeq()
      for (call <- aggCalls) {
        var arguments: IndexedSeq[Any] = IndexedSeq[Any]()
        for (row <- table) {
          arguments :+= call.getArgument(row)
        }

        tuple :+= arguments.reduce(call.reduce)
      }

      data :+= tuple
      vids :+= IndexedSeq(0L)
      return vids
    }

    //Creating hashMap for keys
    val groupList = groupSet.asList()
    var groupMap: mutable.HashMap[Tuple, IndexedSeq[Tuple]] = mutable.HashMap()
    for (row <- table) {
      var key: Tuple = IndexedSeq()
      for (i <- 0 until groupList.size()) {
        key :+= row(groupList.get(i))
      }

      if (!groupMap.contains(key)) {
        groupMap += (key -> IndexedSeq(row))
      } else {
        groupMap(key) :+= row
      }
    }

    //Aggregating
    for (key <- groupMap.keySet) {
      var ret_tuple = key
      for (call <- aggCalls) {
        var arguments: IndexedSeq[Any] = IndexedSeq[Any]()
        for (row <- groupMap(key)) {
          arguments :+= call.getArgument(row)
        }

        ret_tuple :+= arguments.reduce(call.reduce)
      }

      vids :+= IndexedSeq(data.size.toLong)
      data :+= ret_tuple
    }

    vids
  }

  private lazy val evals = new LazyEvaluatorAccess(List.tabulate(aggCalls.size + groupSet.asList().size)(elem => vid => data(vid.toInt)(elem)))

  override def evaluators(): LazyEvaluatorAccess = evals
}
