package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Tuple}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import org.apache.calcite.rex.RexNode

class Filter protected (input: Operator, condition: RexNode) extends skeleton.Filter[Operator](input, condition) with Operator {
  override def open(): Unit = {
    input.open()
  }

  lazy val e: Tuple => Any = eval(condition, input.getRowType)

  override def next(): Block = {
    var block = input.next()
    if (block == null)  {
      return null
    }
    while (block != null) {
      var exclude : Block = IndexedSeq()
      for (row <- block)  {
        if (!e(row).asInstanceOf[Boolean]) {
          exclude :+= row
        }
      }
      if (exclude.size == block.size)  {
        block = input.next()
        if (block == null)  {
          return null
        }
      }
      else  {
        block = block.diff(exclude)
        return block
      }
    }

    block
  }

  override def close(): Unit =  {
    input.close()
  }

}
