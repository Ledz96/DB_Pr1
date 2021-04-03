package ch.epfl.dias.cs422.rel.early.blockatatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Block, Column, PAXPage, Tuple, blockSize}
import ch.epfl.dias.cs422.helpers.rel.early.blockatatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected (cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {
  protected lazy val store: Store = tableToStore(table.unwrap(classOf[ScannableTable]))

  protected var index : Int = 0
  protected var localColumnTable : IndexedSeq[Column] = IndexedSeq()
  protected var localPAXTable : IndexedSeq[PAXPage] = IndexedSeq()

  override def open(): Unit = {
    index = 0
    localColumnTable = IndexedSeq()

    store match {
      case c: ColumnStore =>
        if (store.getRowCount == 0) {
          return
        }

        for (i <- 0 until table.unwrap(classOf[ScannableTable]).schema.size) {
          localColumnTable :+= c.getColumn(i)
        };

      case p: PAXStore =>
        if (store.getRowCount == 0) {
          return
        }

        val minipageSizeMax = p.getPAXPage(0)(0).size
        for (i <- 0 until math.ceil(p.getRowCount.toDouble/minipageSizeMax.toDouble).toInt) {
          localPAXTable :+= p.getPAXPage(i)
        };

      case _ =>
    }
  }

  override def next(): Block = {
    if (index >= store.getRowCount) {
      return null
    }
    var block : Block = IndexedSeq()
    store match {
      case r: RowStore =>
        for (_ <- 0 until blockSize)  {
          if (index < r.getRowCount) {
            block :+= r.getRow(index)
          }
          index += 1
        };
      case _: ColumnStore =>
        for (_ <- 0 until blockSize)  {
          if (index < store.getRowCount) {
            block :+= getTupleFromColumn(index)
          }
          index += 1
        };
      case _: PAXStore =>
        for (_ <- 0 until blockSize) {
          if (index < store.getRowCount) {
            block :+= getTupleFromPAXPage(index)
          }
          index += 1
        };
      case _ =>
    }

    if (block.size <= 0) {
      return block
    }

    block
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
