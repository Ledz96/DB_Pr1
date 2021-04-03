package ch.epfl.dias.cs422.rel.late.operatoratatime

import ch.epfl.dias.cs422.helpers.builder.skeleton
import ch.epfl.dias.cs422.helpers.rel.RelOperator.{Column, PAXPage, Tuple}
import ch.epfl.dias.cs422.helpers.rel.late.LazyEvaluatorAccess
import ch.epfl.dias.cs422.helpers.rel.late.operatoratatime.Operator
import ch.epfl.dias.cs422.helpers.store.{ColumnStore, PAXStore, RowStore, ScannableTable, Store}
import org.apache.calcite.plan.{RelOptCluster, RelOptTable, RelTraitSet}

class Scan protected(cluster: RelOptCluster, traitSet: RelTraitSet, table: RelOptTable, tableToStore: ScannableTable => Store) extends skeleton.Scan[Operator](cluster, traitSet, table) with Operator {

  var data : IndexedSeq[Tuple] = IndexedSeq()
  var localColumnTable : IndexedSeq[Column] = IndexedSeq()
  var localPAXTable : IndexedSeq[PAXPage] = IndexedSeq()

  override def execute(): IndexedSeq[Column] = {                      //Should be VIDs <- IndexedSeq[IndexedSeq(VID), IndexedSeq(VID)...
    val store = tableToStore(table.unwrap(classOf[ScannableTable]))
    var vids : IndexedSeq[Column] = IndexedSeq[Column]()

    if (store.getRowCount == 0) {
      return vids
    }

    store match {
      case r: RowStore =>
        for (i <- 0L until r.getRowCount) {
          data :+= r.getRow(i.intValue())
          val t = IndexedSeq(i)
          vids :+= t
        };
      case c: ColumnStore =>
        localColumnTable = IndexedSeq[Column]()
        for (i <- 0 until table.unwrap(classOf[ScannableTable]).schema.size) {
          localColumnTable :+= c.getColumn(i)
        }

        for (i <- 0L until store.getRowCount) {
          data :+= getTupleFromColumn(i.intValue())
          val t = IndexedSeq(i)
          vids :+= t
        };
      case p: PAXStore =>
        val minipageSizeMax = p.getPAXPage(0)(0).size

        for (i <- 0 until math.ceil(p.getRowCount.toDouble/minipageSizeMax.toDouble).toInt) {
          localPAXTable :+= p.getPAXPage(i)
        }

        for (i <- 0L until store.getRowCount) {
          data :+= getTupleFromPAXPage(i.intValue())
          val t = IndexedSeq(i)
          vids :+= t
        };
      case _ =>
    }

    vids
  }

  def getTupleFromPAXPage(index : Int) : Tuple = {
    val minipageSizeMax = localPAXTable(0)(0).size
    val pageIndex = index / minipageSizeMax
    val minipageIndex = index % minipageSizeMax

    localPAXTable(pageIndex).map(minipage => minipage(minipageIndex))
  }

  def getTupleFromColumn(index : Int) : Tuple = {
    localColumnTable.map(column => column(index)).toIndexedSeq
  }

  private lazy val evals = new LazyEvaluatorAccess(List.tabulate(table.unwrap(classOf[ScannableTable]).schema.size)(elem => vid => data(vid.toInt)(elem)))

  override def evaluators(): LazyEvaluatorAccess = evals
}
