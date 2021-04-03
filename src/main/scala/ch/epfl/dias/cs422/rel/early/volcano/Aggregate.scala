package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet
import scala.collection.mutable

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {

  var table : IndexedSeq[Tuple] = IndexedSeq()
  var finished : Boolean = false
  var index : Integer = 0
  var groupMap : mutable.HashMap[Tuple, IndexedSeq[Tuple]] = mutable.HashMap()
  var keyList : IndexedSeq[Tuple] = IndexedSeq()

  override def open(): Unit = {
    //Declarations and assignations
    val groupList = groupSet.asList()
    input.open()
    finished = false
    index = 0
    groupMap = mutable.HashMap[Tuple, IndexedSeq[Tuple]]()
    keyList = IndexedSeq()
    table = IndexedSeq[Tuple]()

    //Building the full table
    var row = input.next()
    while (row != null)  {
      table :+= row
      row = input.next()
    }

    if (groupList.size() == 0) {
      return
    }

    //Creating the hashMap for keys
    for (row <- table)  {
      var key : Tuple = IndexedSeq()
      for (i <- 0 until groupList.size())  {
        key :+= row(groupList.get(i))
      }

      if (!groupMap.contains(key)) {
        keyList :+= key
        groupMap += (key -> IndexedSeq(row))
      } else {
        groupMap(key) :+= row
      }
    }
  }

  override def next(): Tuple = {
    if (finished) {
      return null
    }

    //Empty table case
    var result: Tuple = IndexedSeq[Any]()
    if (table.isEmpty) {
      for (call <- aggCalls) {
        result :+= call.emptyValue
      }

      finished = true
      return result
    }

    //Empty groupSet case
    if (groupSet.isEmpty) {
      for (call <- aggCalls) {
        var arguments: IndexedSeq[Any] = IndexedSeq[Any]()
        for (row <- table) {
          arguments :+= call.getArgument(row)
        }

        result :+= arguments.reduce(call.reduce)
      }

      finished = true
      return result
    }





    val key = keyList(index)
    var ret_tuple = key
    for (call <- aggCalls)  {
      var arguments: IndexedSeq[Any] = IndexedSeq[Any]()
      for (row <- groupMap(key))  {
        arguments :+= call.getArgument(row)
      }

      ret_tuple :+= arguments.reduce(call.reduce)
    }

    index += 1
    if (index == keyList.size)  {
      finished = true
    }

    ret_tuple
  }

  override def close(): Unit = {
    input.close()
  }
}
