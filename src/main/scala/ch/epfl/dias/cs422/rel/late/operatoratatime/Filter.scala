package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.{Evaluator, LazyEvaluatorRoot}
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected(input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def execute(): IndexedSeq[Column] = {
    val oldVids = input.execute()
    if (oldVids.isEmpty) {
      return oldVids
    }
    var vids = IndexedSeq[Column]()
    for (oldVid <- oldVids) {
      val tuple = input.evaluators().apply(oldVid)
      if (e(tuple).asInstanceOf[Boolean]) {
        vids :+= oldVid
      }
    }

    vids
  }

  override def evaluators(): LazyEvaluatorRoot = input.evaluators()
}
