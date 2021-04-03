package ch.epfl.dias.cs422.rel.early.volcano

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, PAXPage, Tuple, blockSize}
import ch.epfl.dias.cs422.helpers.rel.early.volcano.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected val scannable: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  protected var index : Int = 0
  var localColumnTable : IndexedSeq[Column] = IndexedSeq()
  var localPAXTable : IndexedSeq[PAXPage] = IndexedSeq()

  override def open(): Unit = {
    index = 0
    localColumnTable = IndexedSeq()
    localPAXTable = IndexedSeq()

    scannable match {
      case c: ColumnStore =>
        if (scannable.getRowCount == 0) {
          return
        }

        for (i <- 0 until table.unwrap(classOf[ScannableTable]).schema.size) {
          localColumnTable :+= c.getColumn(i)
        };
      case p: PAXStore =>
        if (scannable.getRowCount == 0) {
          return
        }

        val minipageSizeMax = p.getPAXPage(0)(0).size
        for (i <- 0 until math.ceil(p.getRowCount.toDouble/minipageSizeMax.toDouble).toInt) {
          localPAXTable :+= p.getPAXPage(i)
        };
      case _ =>
    }
  }

  override def next(): Tuple = {
    val row = scannable match {
      case r: RowStore =>      if (index >= scannable.getRowCount) {null} else {r.getRow(index)};
      case _: ColumnStore =>   if (index >= scannable.getRowCount) {null} else {getTupleFromColumn(index)};
      case _: PAXStore =>      if (index >= scannable.getRowCount) {null} else {getTupleFromPAXPage(index)};
      case _ => null
    }
    index+=1

    row
  }

  def getTupleFromPAXPage(index : Int) : Tuple = {
    val minipageSizeMax = localPAXTable(0)(0).size
    val pageIndex = index / minipageSizeMax
    val minipageIndex = index % minipageSizeMax

    localPAXTable(pageIndex).map(minipage => minipage(minipageIndex))
  }

  def getTupleFromColumn(index : Int) : Tuple = {
    localColumnTable.map(column => column(index))
  }

  override def close(): Unit = {}
}
