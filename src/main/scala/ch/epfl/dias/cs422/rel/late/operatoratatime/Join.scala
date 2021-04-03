package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorRoot
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable

class Join(left: Operator, right: Operator, condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {
  override def execute(): IndexedSeq[Column] = {
    val oldLeftVids = left.execute()
    if (oldLeftVids.isEmpty) {
      return oldLeftVids
    }
    val oldRightVids = right.execute()
    if (oldRightVids.isEmpty) {
      return oldRightVids
    }

    var leftTable = IndexedSeq[Tuple]()
    var rightTable = IndexedSeq[Tuple]()
    for (leftVid <- oldLeftVids) {
      leftTable :+= left.evaluators().apply(leftVid) :+ leftVid
    }
    for (rightVid <- oldRightVids) {
      rightTable :+= right.evaluators().apply(rightVid) :+ rightVid
    }
    var vids = IndexedSeq[Column]()

    var hashTable : mutable.HashMap[Tuple, IndexedSeq[Tuple]] = mutable.HashMap()
    for (row <- leftTable) {
      val keys = getLeftKeys.map(i => row(i))
      if (!hashTable.contains(keys)) {
        hashTable += (keys -> IndexedSeq(row))
      } else {
        hashTable(keys) :+= row
      }
    }

    for (row <- rightTable) {
      val key = getRightKeys.map(i => row(i))
      if (hashTable.contains(key)) {
        for (key_row <- hashTable(key)) {
          var pair = IndexedSeq[Any]()
          pair :+= key_row(key_row.size-1)
          pair :+= row(row.size-1)
          vids :+= pair
        }
      }
    }

    vids
  }

  private lazy val evals = lazyEval(left.evaluators(), right.evaluators(), left.getRowType, right.getRowType)
  override def evaluators(): LazyEvaluatorRoot = evals
}
