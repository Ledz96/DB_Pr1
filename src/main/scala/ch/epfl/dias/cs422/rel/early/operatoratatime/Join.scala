package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  var columns : IndexedSeq[Column] = IndexedSeq()
  var table : IndexedSeq[Tuple] = IndexedSeq()
  override def execute(): IndexedSeq[Column] = {
    var hashTable : mutable.HashMap[Tuple, List[Tuple]] = mutable.HashMap()
    val leftColumns = left.execute()
    val rightColumns = right.execute()
    if (leftColumns.isEmpty) {
      return leftColumns
    }
    if (rightColumns.isEmpty)  {
      return rightColumns
    }

    var leftTable : IndexedSeq[Tuple] = IndexedSeq()
    var rightTable : IndexedSeq[Tuple] = IndexedSeq()
    var mergedTable : IndexedSeq[Tuple] = IndexedSeq()
    var res : IndexedSeq[Column] = IndexedSeq()

    columns = leftColumns
    for (i <- 0 until columns(0).size) {
      leftTable :+= getTupleFromColumn(i)
    }
    columns = rightColumns
    for (i <- 0 until columns(0).size) {
      rightTable :+= getTupleFromColumn(i)
    }

    var keys : Tuple = IndexedSeq()
    for (row <- leftTable) {
      keys = getLeftKeys.map(i => row(i))
      if (!hashTable.contains(keys)) {
        hashTable += (keys -> (row :: Nil))
      } else {
        hashTable(keys) ::= row
      }
    }

    var key : Tuple = IndexedSeq()
    for (row <- rightTable) {
      key = getRightKeys.map(i => row(i))

      if (hashTable.contains(key)) {
        for (key_row <- hashTable(key)) {
          mergedTable :+= (key_row ++ row)
        }
      }
    }

    if (mergedTable.isEmpty) {
      return mergedTable
    }
    table = mergedTable
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