package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.Tuple
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import org.apache.calcite.rex.RexNode
import scala.collection.mutable

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  var right_row : Tuple = _
  var hashTable : mutable.HashMap[Tuple, IndexedSeq[Tuple]] = mutable.HashMap()
  var groupSize : Integer = 0
  var key : Tuple = _
  var positionInGroup : Integer = 0

  override def open(): Unit = {
    left.open()
    right.open()
    positionInGroup = 0
    groupSize = 0

    var row = left.next()
    while (row != null) {
      var keys = getLeftKeys.map(i => row(i))
      if (!hashTable.contains(keys)) {
        hashTable += (keys -> IndexedSeq(row))
      } else {
        hashTable(keys) :+= row
      }
      row = left.next()
    }
  }

  override def next(): Tuple = {
    while (positionInGroup >= groupSize) {
      right_row = right.next()
      if (right_row == null)  {
        return null
      }
      key = getRightKeys.map(i => right_row(i))
      if (hashTable.contains(key)) {
        groupSize = hashTable(key).size
        positionInGroup = 0
      }
    }

    val row = hashTable(key)(positionInGroup) ++ right_row
    positionInGroup += 1
    row
  }

  override def close(): Unit = {
    left.close()
    right.close()
  }

}
