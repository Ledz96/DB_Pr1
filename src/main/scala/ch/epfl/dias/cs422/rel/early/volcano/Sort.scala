package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.{RexLiteral, RexNode}

class Sort protected (input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  var table: IndexedSeq[Tuple] = IndexedSeq()
  var index: Integer = 0
  var limit: Integer = -1

  override def open(): Unit = {
    if (fetch != null) {
      limit = RexLiteral.intValue(fetch)
      if (limit == 0) {
        return
      }
    }
    index = 0
    input.open()

    var row = input.next()
    if (row == null)  {
      table = null
    }
    while (row != null) {
      table :+= row
      row = input.next()
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
      index = RexLiteral.intValue(offset)
    }
  }

  override def next(): Tuple = {
    if (table == null || index >= table.size || limit == 0)  {
      return null
    }
    val row = table(index)
    index += 1
    limit -= 1
    row
  }

  override def close(): Unit = {
    input.close()
  }
}
