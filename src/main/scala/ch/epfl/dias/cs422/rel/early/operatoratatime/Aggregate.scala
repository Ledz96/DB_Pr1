package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.rex.AggregateCall
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.mutable

class Aggregate protected(input: Operator, groupSet: ImmutableBitSet, aggCalls: List[AggregateCall]) extends skeleton.Aggregate[Operator](input, groupSet, aggCalls) with Operator {
  var columns : IndexedSeq[Column] = IndexedSeq()
  var table : IndexedSeq[Tuple] = IndexedSeq()
  override def execute(): IndexedSeq[Column] = {
    //Declarations and assignations
    table = IndexedSeq()
    var groupMap : mutable.HashMap[Tuple, List[Tuple]] = mutable.HashMap()
    var result: Tuple = IndexedSeq[Any]()
    var res : IndexedSeq[Column] = IndexedSeq()

    columns = input.execute()
    //Empty table case
    if (columns.isEmpty) {
      for (call <- aggCalls) {
        result :+= call.emptyValue
      }

      for (i <- 0 until result.size) {
        var t = IndexedSeq(result(i))
        res :+= t
      }

      return res
    }

    for (i <- 0 until columns(0).size) {
      table :+= getTupleFromColumn(i)
    }

    //Empty groupset
    if (groupSet.isEmpty) {
      for (call <- aggCalls) {
        var arguments: List[Any] = List[Any]()
        for (row <- table) {
          arguments :+= call.getArgument(row)
        }

        result :+= arguments.reduce(call.reduce)
      }

      for (i <- 0 until result.size) {
        var t = IndexedSeq(result(i))
        res :+= t
      }

      return res
    }

    val groupList = groupSet.asList()

    //Creating the hashMap for keys
    for (row <- table)  {
      var key : Tuple = IndexedSeq()
      for (i <- 0 until groupList.size())  {
        key :+= row(groupList.get(i))
      }

      if (!groupMap.contains(key)) {
        groupMap += (key -> (row :: Nil))
      } else {
        groupMap(key) ::= row
      }
    }

    //Aggregating

    table = IndexedSeq()
    for (key <- groupMap.keySet) {
      var ret_tuple = key
      for (call <- aggCalls) {
        var arguments: List[Any] = List[Any]()
        arguments.map(a => call.emptyValue)
        for (row <- groupMap(key)) {
          arguments ::= call.getArgument(row)
        }

        ret_tuple :+= arguments.reduce(call.reduce)
      }
      table :+= ret_tuple
    }

    for (i <- 0 until table(0).size) {
      res :+= getColumnFromTuple(i)
    }

    res
  }

  def getTupleFromColumn(index : Int) : Tuple = {
    columns.map(column => column(index))
  }

  def getColumnFromTuple(index : Int) : Column = {
    table.map(tuple => tuple(index))
  }
}
