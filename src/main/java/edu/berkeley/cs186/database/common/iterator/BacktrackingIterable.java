package edu.berkeley.cs186.database.common.iterator;

/**
 * Abstraction of the ability of backtracking
 * @param <T>
 */
public interface BacktrackingIterable<T> extends Iterable<T> {

    /**
     * Returns a backtracking iterator over elements of type T.
     * @return a backtracking iterator
     */
    @Override
    BacktrackingIterator<T> iterator();
}
