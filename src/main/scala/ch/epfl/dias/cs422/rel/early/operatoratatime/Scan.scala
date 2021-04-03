package ch.epfl.dias.cs422.rel.early.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, PAXPage, Tuple, blockSize}
import ch.epfl.dias.cs422.helpers.rel.early.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  var rowTable : IndexedSeq[Tuple] = IndexedSeq()
  var localPAXTable : IndexedSeq[PAXPage] = IndexedSeq()

  override def execute(): IndexedSeq[Column] = {
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))

    var result : IndexedSeq[Column] = IndexedSeq()
    if (store.getRowCount == 0) {
      return result
    }
    store match {
      case r: RowStore =>
        for (i <- 0L until r.getRowCount) {
          rowTable :+= r.getRow(i.intValue())
        }
        for (i <- 0 until rowTable(0).size) {
          result :+= getColumnFromTuple(i)
        }
      case c: ColumnStore =>
        for (i <- 0 until table.unwrap(classOf[ScannableTable]).schema.size) {
          result :+= c.getColumn(i)
        };
      case p: PAXStore =>
        val minipageSizeMax = p.getPAXPage(0)(0).size
        for (i <- 0 until math.ceil(p.getRowCount.toDouble/minipageSizeMax.toDouble).toInt) {
          localPAXTable :+= p.getPAXPage(i)
        };
        for (i <- 0 until localPAXTable(0).size) {
          result :+= getColumnFromPAXPage(i)
        }
      case _ =>
    }

    result
  }

  def getColumnFromTuple(index : Int) : Column = {
    rowTable.map(tuple => tuple(index))
  }

  def getColumnFromPAXPage(index: Int): Column = {
    localPAXTable.map(page => page(index)).reduce((a, b) => a ++ b)
  }
}
