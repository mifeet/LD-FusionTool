package cz.cuni.mff.odcleanstore.fusiontool.util;

import java.lang.reflect.Array;
import java.util.*;

public class ClusterIterator<T> implements Iterator<List<T>> {
    private final T[] sortedElements;
    private final Comparator<T> comparator;
    private int cursor = 0;

    @SuppressWarnings("unchecked")
    public ClusterIterator(List<T> elements, Comparator<T> comparator) {
        this.comparator = comparator;
        if (elements.isEmpty()) {
            this.sortedElements = (T[]) new Object[0];
        } else {
            T[] elementsArray = (T[]) Array.newInstance(elements.get(0).getClass(), elements.size());
            this.sortedElements = elements.toArray(elementsArray);
            Arrays.sort(this.sortedElements, comparator);
        }
    }

    @Override
    public boolean hasNext() {
        return hasNextElement();
    }

    @Override
    public List<T> next() {
        int fromIndex = cursor;
        T first = nextElement();
        while (hasNextElement()) {
            T next = peekNextElement();
            if (comparator.compare(first, next) == 0) {
                nextElement();
            } else {
                break; // We reached the next cluster
            }
        }
        return new SubList(fromIndex, cursor);
    }

    private boolean hasNextElement() {
        return cursor < sortedElements.length;
    }

    private T peekNextElement() {
        return sortedElements[cursor];
    }

    private T nextElement() {
        return sortedElements[cursor++];
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Call to remove() is not supported.");
    }

    /**
     * Sublist of wrapped elements determined by start and end index into {@link #sortedElements} representing one cluster.
     */
    protected class SubList extends AbstractList<T> implements RandomAccess {
        private int from;
        private int to;

        /**
         * @param from index of the first item represented by this sublist into {@link #sortedElements}.
         * @param toExclusive index past the last item represented by this sublist into
         *      {@link #sortedElements}.
         */
        SubList(int from, int toExclusive) {
            this.from = from;
            this.to = toExclusive;
        }

        @Override
        public T get(int index) {
            if (from + index >= to) {
                throw new IndexOutOfBoundsException();
            }
            return sortedElements[from + index];
        }

        @Override
        public int size() {
            return to - from;
        }

        @Override
        public int indexOf(Object obj) {
            if (obj == null) {
                for (int i = from; i < to; i++) {
                    if (sortedElements[i] == null) {
                        return i;
                    }
                }
            } else {
                for (int i = from; i < to; i++) {
                    if (obj.equals(sortedElements[i])) {
                        return i;
                    }
                }
            }
            return -1;
        }

        @Override
        public boolean contains(Object obj) {
            return indexOf(obj) != -1;
        }
    }
}
