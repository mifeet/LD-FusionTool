/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.util;

import java.util.Iterator;

/**
 * Decorates an iterator such that elements of the underlying iterator are converted from type S to type T.
 * @param <T> target type
 * @param <S> source type
 * @author Jan Michelfeit
 */
public abstract class ConvertingIterator<S, T> implements Iterator<T> {
    private final Iterator<S> iterator;
    
    /**
     * Constructs a new instance for the given iterator and converter.
     * @param iterator    the iterator to use
     *
     */
    public ConvertingIterator(Iterator<S> iterator) {
        this.iterator = iterator;
    }

    /**
     * Convert object from type S to type T.
     * @param object object to convert
     * @return converted result
     */
    public abstract T convert(S object);

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }
    
    /**
     * Gets the next object from the iteration, transforming it using the
     * current converter.
     * @return the next object
     */
    @Override
    public T next() {
        return convert(iterator.next());
    }

    @Override
    public void remove() {
        iterator.remove();
    }
}