package cz.cuni.mff.odcleanstore.crbatch.loaders;

import com.hp.hpl.jena.graph.Node;

import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;

/**
 * Iterator over URI resources.
 * @author Jan Michelfeit
 */
public interface NodeIterator extends Closeable {
    /**
     * Returns {@code true} if the iteration has more elements.
     * @return {@code true} if the iteration has more elements
     */
    boolean hasNext();

    /**
     * Returns the next element in the iteration.
     * @return the next element in the iteration
     * @throws CRBatchException error
     */
    Node next() throws CRBatchException;
}
