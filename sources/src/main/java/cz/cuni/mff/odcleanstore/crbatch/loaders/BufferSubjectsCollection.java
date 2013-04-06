package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.util.HashSet;
import java.util.Iterator;

/**
 * Collection over triple subjects discovered during traversing of triples.
 * The collection behaves like a Set for addition of new URIs.
 * The class DOES NOT keep track of nodes that were added and/or removed to the queue.
 * Nodes other than resource URIs or blank nodes are discarded.
 * @author Jan Michelfeit
 */
public class BufferSubjectsCollection implements UriCollection {
    private final HashSet<String> uriQueue = new HashSet<String>();

    @Override
    public boolean hasNext() {
        return !uriQueue.isEmpty();
    }

    @Override
    public String next() {
        Iterator<String> it = uriQueue.iterator();
        String result = it.next();
        it.remove();
        return result;
    }

    @Override
    public void close() {
    }

    @Override
    public void add(String uri) {
        uriQueue.add(uri);
    }
}
