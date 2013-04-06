/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.util;

import java.util.Iterator;

/**
 * Decorates an iterator such that elements of the underlying iterator are converted from type S to type T.
 * @param <T> target type
 * @param <S> source type
 * @author Jan Michelfeit
 */
public class ConvertingIterator<S, T> implements Iterator<T> {
    /** The iterator to use. */
    private final Iterator<S> iterator;
    
    /** The converter to use. */
    private final GenericConverter<S, T> converter;

    /**
     * Returns the given iterator decorated with {@link ConvertingIterator}.
     * @param iterator iterator to decorate
     * @param converter converter to use
     * @return decorated iterator
     * @param <T> target type
     * @param <S> source type
     */
    public static <S, T> ConvertingIterator<S, T> decorate(Iterator<S> iterator, GenericConverter<S, T> converter) {
        return new ConvertingIterator<S, T>(iterator, converter);
    }
    
    /**
     * Constructs a new instance for the given iterator and converter.
     * @param iterator    the iterator to use
     * @param converter the converter to use
     */
    public ConvertingIterator(Iterator<S> iterator, GenericConverter<S, T> converter) {
        this.iterator = iterator;
        this.converter = converter;
    }

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
        return converter.convert(iterator.next());
    }

    @Override
    public void remove() {
        iterator.remove();
    }

    /**
     * Returns the wrapped iterator.
     * @return the wrapped iterator
     */
    public Iterator<S> getIterator() {
        return iterator;
    }

    /**
     * Returns the used converter.
     * @return the converter
     */
    public GenericConverter<S, T> getConverter() {
        return converter;
    }
}