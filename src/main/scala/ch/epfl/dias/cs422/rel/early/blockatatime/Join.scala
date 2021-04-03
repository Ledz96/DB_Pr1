package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

import scala.collection.mutable

class Join(left: Operator,
           right: Operator,
           condition: RexNode) extends skeleton.Join[Operator](left, right, condition) with Operator {

  var right_block: Block = _
  var hashTable: mutable.HashMap[Tuple, List[Tuple]] = mutable.HashMap()
  var groupSize: Integer = 0
  var key: Tuple = _
  var positionInGroup: Integer = 0
  var blockIndex = 0

  override def open(): Unit = {
    left.open()
    right.open()
    positionInGroup = 0
    groupSize = 0
    blockIndex = 0

    var block = left.next()
    while (block != null) {
      for (row <- block)  {
        var keys = getLeftKeys.map(i => row(i))
        if (!hashTable.contains(keys)) {
          hashTable += (keys -> (row :: Nil))
        } else {
          hashTable(keys) ::= row
        }
      }
      block = left.next()
    }

    right_block = right.next()
  }

  override def next(): Block = {
    var block: Block = IndexedSeq()
    while (right_block != null) {
      while (blockIndex < right_block.size) {
        key = getRightKeys.map(t => right_block(blockIndex)(t))
        if (hashTable.contains(key)) {
          groupSize = hashTable(key).size
          while (positionInGroup < groupSize) {
            block :+= (hashTable(key)(positionInGroup) ++ right_block(blockIndex))
            positionInGroup += 1
            if (block.size == blockSize) {
              return block
            }
          }
          positionInGroup = 0
        }
        blockIndex += 1
      }
      right_block = right.next()
      blockIndex = 0
    }

    if (block.isEmpty)  {
      return null
    }

    block
  }

  override def close(): Unit = {
    left.close()
    right.close()
  }

}

