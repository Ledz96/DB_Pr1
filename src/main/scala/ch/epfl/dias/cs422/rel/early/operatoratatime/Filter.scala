package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  lazy val e: Tuple => Any = eval(condition, input.getRowType)
  var columns : IndexedSeq[Column] = IndexedSeq()
  var table : IndexedSeq[Tuple] = IndexedSeq()
  override def execute(): IndexedSeq[Column] = {
    columns = input.execute()
    if (columns.isEmpty)  {
      return columns
    }

    table = IndexedSeq()

    for (i <- 0 until columns(0).size) {
      table :+= getTupleFromColumn(i)
    }

    var exclude = IndexedSeq[Tuple]()

    for (row <- table) {
      if (!e(row).asInstanceOf[Boolean]) {
        exclude :+= row
      }
    }
    table = table.diff(exclude)

    if (table.isEmpty) {
      return table
    }

    var res : IndexedSeq[Column] = IndexedSeq()
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
