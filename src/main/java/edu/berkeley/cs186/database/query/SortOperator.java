package edu.berkeley.cs186.database.query;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.disk.Run;
import edu.berkeley.cs186.database.table.Record;
import edu.berkeley.cs186.database.table.Schema;
import edu.berkeley.cs186.database.table.stats.TableStats;

import java.util.*;

public class SortOperator extends QueryOperator {
    private final TransactionContext transaction;
    private final int numBuffers;
    private final int sortColumnIndex;
    private final String sortColumnName;
    protected Comparator<Record> comparator;
    private Run sortedRecords;

    public SortOperator(TransactionContext transaction, QueryOperator source,
                        String columnName) {
        super(OperatorType.SORT, source);
        this.transaction = transaction;
        this.numBuffers = this.transaction.getWorkMemSize();
        this.sortColumnIndex = getSchema().findField(columnName);
        this.sortColumnName = getSchema().getFieldName(this.sortColumnIndex);
        this.comparator = new RecordComparator();
    }

    @Override
    public TableStats estimateStats() {
        return getSource().estimateStats();
    }

    @Override
    public Schema computeSchema() {
        return getSource().getSchema();
    }

    @Override
    public int estimateIOCost() {
        int n = getSource().estimateStats().getNumPages();
        double pass0Runs = Math.ceil(n / (double) numBuffers);
        double numPasses = 1 + Math.ceil(Math.log(pass0Runs) / Math.log(numBuffers - 1));
        return (int) (2 * n * numPasses) + getSource().estimateIOCost();
    }

    @Override
    public String str() {
        return "Sort (cost=" + estimateIOCost() + ")";
    }

    @Override
    public List<String> sortedBy() {
        return Collections.singletonList(sortColumnName);
    }

    @Override
    public boolean materialized() {
        return true;
    }

    @Override
    public BacktrackingIterator<Record> backtrackingIterator() {
        if (this.sortedRecords == null) {
            this.sortedRecords = sort();
        }
        return sortedRecords.iterator();
    }

    @Override
    public Iterator<Record> iterator() {
        return backtrackingIterator();
    }

    /**
     * Returns a Run containing records from the input iterator in sorted order.
     * You're free to use an in memory sort over all the records using one of
     * Java's built-in sorting methods.
     *
     * @return a single sorted run containing all the records from the input
     * iterator
     */
    public Run sortRun(Iterator<Record> records) {
        // TODO(proj3_part1): implement
        final Run sortedRun = makeRun();
        final List<Record> recordList = new ArrayList<>();
        while (records.hasNext()) {
            Record rcd = records.next();
            recordList.add(rcd);
        }
        recordList.sort(comparator);
        sortedRun.addAll(recordList);
        return sortedRun;
    }

    /**
     * Given a list of sorted runs, returns a new run that is the result of
     * merging the input runs. You should use a Priority Queue (java.util.PriorityQueue)
     * to determine which record should be should be added to the output run
     * next.
     * <p>
     * You are NOT allowed to have more than runs.size() records in your
     * priority queue at a given moment. It is recommended that your Priority
     * Queue hold Pair<Record, Integer> objects where a Pair (r, i) is the
     * Record r with the smallest value you are sorting on currently unmerged
     * from run i. `i` can be useful to locate which record to add to the queue
     * next after the smallest element is removed.
     *
     * @param runs ensure every passed in run is sorted
     * @return a single sorted run obtained by merging the input runs
     */
    public Run mergeSortedRuns(List<Run> runs) {
        final int runsSize = runs.size();
        if ((runsSize > this.numBuffers - 1)) {
            throw new IllegalStateException("Runs out of buffer range.");
        }
        // TODO(proj3_part1): implement
        //to avoid too large memory overhead,we firstly push the first record in each run,
        //then use a mechanism which is similar to the "sliding window"
        final Run mergedRun = makeRun();
        final Queue<Pair<Record, Integer>> window = new PriorityQueue<>(runsSize, new RecordPairComparator());
        //Iterators over each corresponding run,for the further run unique traverse
        final ArrayList<Iterator<Record>> corrIterators = new ArrayList<>();
        for (int i = 0; i < runsSize; i++) {
            final BacktrackingIterator<Record> rcdIterator = runs.get(i).iterator();
            if (rcdIterator.hasNext()) {
                window.offer(new Pair<>(rcdIterator.next(), i));
            }
            corrIterators.add(rcdIterator);
        }
        while (!window.isEmpty()) {
            final Pair<Record, Integer> pair = window.poll();
            final Record rcd = pair.getFirst();
            final Integer index = pair.getSecond();
            final Iterator<Record> iterator = corrIterators.get(index);
            if (iterator.hasNext()) {
                window.offer(new Pair<>(iterator.next(), index));
            }
            mergedRun.add(rcd);
        }
        return mergedRun;
    }

    /**
     * Given a list of N sorted runs, returns a list of sorted runs that is the
     * result of merging (numBuffers - 1) of the input runs at a time. If N is
     * not a perfect multiple of (numBuffers - 1) the last sorted run should be
     * the result of merging less than (numBuffers - 1) runs.
     *
     * @return a list of sorted runs obtained by merging the input runs
     */
    public List<Run> mergePass(List<Run> runs) {
        // TODO(proj3_part1): implement
        final int runsSize = runs.size();
        final int nBuff = numBuffers - 1;
        final int nCompleteSubLst = runsSize / nBuff;
        final List<Run> mergeRet = new ArrayList<>(nCompleteSubLst + 1);
        int i = 0;
        while (i < nCompleteSubLst) {
            int currIdx = i * nBuff;
            mergeRet.add(mergeSortedRuns(runs.subList(currIdx, currIdx + nBuff)));
            ++i;
        }
        if (runsSize % nBuff != 0) {
            mergeRet.add(mergeSortedRuns(runs.subList(i * nBuff, runsSize)));
        }
        return mergeRet;
    }

    /**
     * Does an external merge sort over the records of the source operator.
     * You may find the getBlockIterator method of the QueryOperator class useful
     * here to create your initial set of sorted runs.
     *
     * @return a single run containing all of the source operator's records in
     * sorted order.
     * @see QueryOperator#getBlockIterator(Iterator, Schema, int)
     */
    public Run sort() {
        // Iterator over the records of the relation we want to sort
        Iterator<Record> sourceIterator = getSource().iterator();
        // TODO(proj3_part1): implement
        List<Run> sortedRuns = new ArrayList<>();
        while (sourceIterator.hasNext()) {
            sortedRuns.add(sortRun(getBlockIterator(sourceIterator,getSchema(),numBuffers)));
        }
        while (sortedRuns.size() > 1) {
            sortedRuns = mergePass(sortedRuns);
        }
        assert sortedRuns.size() == 1;
        return sortedRuns.get(0);
    }

    /**
     * @return a new empty run.
     */
    public Run makeRun() {
        return new Run(this.transaction, getSchema());
    }

    /**
     * @param records
     * @return A new run containing the records in `records`
     */
    public Run makeRun(List<Record> records) {
        Run run = new Run(this.transaction, getSchema());
        run.addAll(records);
        return run;
    }

    private class RecordComparator implements Comparator<Record> {
        @Override
        public int compare(Record r1, Record r2) {
            return r1.getValue(sortColumnIndex).compareTo(r2.getValue(sortColumnIndex));
        }
    }

    /**
     * Compares the two (record, integer) pairs based only on the record
     * component using the default comparator. You may find this useful for
     * implementing mergeSortedRuns.
     */
    private class RecordPairComparator implements Comparator<Pair<Record, Integer>> {
        @Override
        public int compare(Pair<Record, Integer> o1, Pair<Record, Integer> o2) {
            return SortOperator.this.comparator.compare(o1.getFirst(), o2.getFirst());
        }
    }
}

