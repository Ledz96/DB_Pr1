package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{LazyEvaluatorAccess, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.ScannableTable
import org.apache.calcite.rel.{RelCollation, RelFieldCollation}
import org.apache.calcite.rex.{RexLiteral, RexNode}

class Sort protected(input: Operator, collation: RelCollation, offset: RexNode, fetch: RexNode) extends skeleton.Sort[Operator](input, collation, offset, fetch) with Operator {
  val oldVids: IndexedSeq[Column] = input.execute()
  val columnSize: Int = if (oldVids.isEmpty) 0 else input.evaluators().apply(oldVids(0)).size
  var table: IndexedSeq[Tuple] = IndexedSeq[Tuple]()
  override def execute(): IndexedSeq[Column] = {
    var limit = -1
    if (fetch != null) {
      limit = RexLiteral.intValue(fetch)
      if (limit == 0) {
        return IndexedSeq[Column]()
      }
    }

    if (oldVids.isEmpty) {
      return oldVids
    }
    table = IndexedSeq[Tuple]()
    var vids = IndexedSeq[Column]()
    for (oldVid <- oldVids) {
      table :+= input.evaluators().apply(oldVid)
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

    var index = 0
    if (offset != null) {
      index = RexLiteral.intValue(offset)
    }
    for (i <- index until table.size) {
      vids :+= IndexedSeq(i.toLong)
      if (limit != -1) {
        limit -= 1
        if (limit == 0) {
          return vids
        }
      }
    }

    vids
  }


  private lazy val evals = new LazyEvaluatorAccess(List.tabulate(columnSize)(elem => vid => table(vid.toInt)(elem)))
  override def evaluators(): LazyEvaluatorAccess = evals
}
