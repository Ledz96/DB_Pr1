package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.{RexLiteral, RexNode}

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var columns : IndexedSeq[Column] = IndexedSeq()
  var table : IndexedSeq[Tuple] = IndexedSeq()

  override def execute(): IndexedSeq[Column] = {
    var limit = -1
    if (fetch != null) {
      limit = RexLiteral.intValue(fetch)
      if (limit == 0) {
        return IndexedSeq[Column]()
      }
    }
    columns = input.execute()
    if (columns.isEmpty)  {
      return columns
    }
    table = IndexedSeq()
    var res : IndexedSeq[Column] = IndexedSeq()

    for (i <- 0 until columns(0).size) {
      table :+= getTupleFromColumn(i)
    }

    val field_collations = collation.getFieldCollations

    for (i <- collation.getFieldCollations.size()-1 to 0 by -1) {
      table = table.sortWith((t1, t2) => {
        if (field_collations.get(i).shortString() == "DESC") {
          RelFieldCollation.compare(t1(field_collations.get(i).getFieldIndex).asInstanceOf[Comparable[_]], t2(field_collations.get(i).getFieldIndex).asInstanceOf[Comparable[_]], 0) > 0
        } else {
          RelFieldCollation.compare(t1(field_collations.get(i).getFieldIndex).asInstanceOf[Comparable[_]], t2(field_collations.get(i).getFieldIndex).asInstanceOf[Comparable[_]], 0) < 0
        }
      })
    }

    if (offset != null) {
      table = table.drop(RexLiteral.intValue(offset))
    }
    if (limit != -1) {
      table = table.take(limit)
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
