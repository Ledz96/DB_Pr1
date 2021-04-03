package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable

class Aggregate protected (input: Operator,
                           groupSet: ImmutableBitSet,
                           aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  var table : List[Tuple] = List()
  var finished : Boolean = false
  var index : Integer = 0
  var groupMap : mutable.HashMap[Tuple, List[Tuple]] = mutable.HashMap()
  var keyList : List[Tuple] = List()

  override def open(): Unit = {
    //Declarations and assignations
    var groupList = groupSet.asList()
    input.open()
    finished = false
    index = 0
    groupMap = mutable.HashMap[Tuple, List[Tuple]]()
    keyList = List()
    table = List[Tuple]()

    //Building the full table
    var block = input.next()
    while (block != null)  {
      for (row <- block) {
        table ::= row
      }
      block = input.next()
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
        keyList ::= key
        groupMap += (key -> (row :: Nil))
      } else {
        groupMap(key) ::= row
      }
    }
  }

  override def next(): Block = {
    if (finished) {
      return null
    }

    //Empty table case
    var result: Block = IndexedSeq()
    if (table.isEmpty) {
      var res_row : Tuple = IndexedSeq()
      for (call <- aggCalls) {
        res_row :+= call.emptyValue
      }

      finished = true
      result :+= res_row
      return result
    }

    //Empty groupSet case
    if (groupSet.isEmpty) {
      var res_row : Tuple = IndexedSeq()
      for (call <- aggCalls) {
        var arguments: List[Any] = List[Any]()
        for (row <- table) {
          arguments :+= call.getArgument(row)
        }

        res_row :+= arguments.reduce(call.reduce)
      }

      finished = true
      result :+= res_row
      return result
    }




    for (_ <- 0 until blockSize) {
      val key = keyList(index)
      var ret_tuple = key
      for (call <- aggCalls) {
        var arguments: List[Any] = List[Any]()
        arguments.map(a => call.emptyValue)
        for (row <- groupMap(key)) {
          arguments ::= call.getArgument(row)
        }

        ret_tuple :+= arguments.reduce(call.reduce)
      }

      result :+= ret_tuple

      index += 1
      if (index == keyList.size) {
        finished = true
        return result
      }
    }

    result
  }

  override def close(): Unit = {
    input.close()
  }
}
