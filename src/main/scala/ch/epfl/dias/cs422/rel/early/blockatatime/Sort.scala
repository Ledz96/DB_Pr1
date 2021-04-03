package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
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

    var block = input.next()
    if (block == null)  {
      table = null
    }
    while (block != null) {
      for (row <- block) {
        table :+= row
      }
      block = input.next()
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

  override def next(): Block = {
    if (table == null || index >= table.size || limit == 0)  {
      return null
    }

    var block : Block = IndexedSeq()
    for (_ <- 0 until blockSize)  {
      block :+= table(index)
      println(block)
      index += 1
      limit -= 1
      if (index >= table.size || limit == 0)  {
        return block
      }
    }

    block
  }

  override def close(): Unit = {
    input.close()
  }

}
