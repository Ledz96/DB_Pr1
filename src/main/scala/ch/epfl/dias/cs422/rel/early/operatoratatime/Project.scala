package ch.epfl.dias.cs422.rel.early.operatoratatime

import java.util

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.RexNode
import scala.jdk.CollectionConverters._


class Project protected(input: Operator, projects: util.List[_ <: RexNode], rowType: RelDataType) extends skeleton.Project[Operator](input, projects, rowType) with Operator {
  lazy val evaluator: Tuple => Tuple = eval(projects.asScala.toIndexedSeq, input.getRowType)
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

    var res : IndexedSeq[Column] = IndexedSeq()

    for (i <- 0 until table.size) {
      table = table.updated(i, evaluator(table(i)))
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
