package cz.cuni.mff.odcleanstore.crbatch.io;

import java.io.IOException;
import java.util.Set;

import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;

/**
 * Factory interface for potentially large collections. 
 * @author Jan Michelfeit
 */
public interface LargeCollectionFactory extends Closeable<IOException> {
    /**
     * Creates a new {@link Set}.
     * @param <T> type of collection
     * @return a new Set
     */
    <T> Set<T> createSet();
}

