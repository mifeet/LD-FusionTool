/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.util;

/**
 * Interface for converting from one type of value to another.
 * @param <T> target type
 * @param <S> source type
 * @author Jan Michelfeit
 */
public interface GenericConverter<S, T> {
    /**
     * Convert object from type S to type T.
     * @param object object to convert 
     * @return converted result
     */
    T convert(S object);
}
