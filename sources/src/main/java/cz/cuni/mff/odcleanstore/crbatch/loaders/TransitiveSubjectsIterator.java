package cz.cuni.mff.odcleanstore.crbatch.loaders;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;

import com.hp.hpl.jena.graph.Node;

import cz.cuni.mff.odcleanstore.crbatch.exceptions.CRBatchException;
import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;
import de.fuberlin.wiwiss.ng4j.Quad;

/**
 * Iterator over triple subjects discovered during traversing of triples.
 * The iterator is implemented as a queue to which new nodes can be added.
 * The class keeps track of all seen nodes and those that were seen before are silently discarded. 
 * Nodes other than resource URIs or bnodes are discarded as well.
 * @author Jan Michelfeit
 */
public class TransitiveSubjectsIterator implements NodeIterator, Closeable {
    //private static final Logger LOG = LoggerFactory.getLogger(TransitiveSubjectsIterator.class);

    private final HashSet<Node> seenResources = new HashSet<Node>(); // Nodes are safe for HashSet use and memory-efficient
    private final Queue<Node> resourceQueue = new ArrayDeque<Node>();

    /**
     * Creates a new instance of the iterator with nodes given by seedResourceIterator as initial nodes in the queue.
     * @param seedResourceIterator iterator over resources to be added to the iteration queue initially
     * @return new instance of {@link TransitiveSubjectsIterator}
     * @throws CRBatchException error when traversing seedResourceIterator
     */
    public static TransitiveSubjectsIterator createTransitiveSubjectsIterator(NodeIterator seedResourceIterator)
            throws CRBatchException {
        TransitiveSubjectsIterator result = new TransitiveSubjectsIterator();
        while (seedResourceIterator.hasNext()) {
            result.tryAddNode(seedResourceIterator.next());
        }
        return result;
    }

    /**
     * Adds all objects from the given quads to the iteration queue.
     * @param quads quads whose objects are added
     */
    public void addObjectsFromQuads(Collection<Quad> quads) {
        for (Quad quad : quads) {
            tryAddNode(quad.getObject());
        }
    }

    private void tryAddNode(Node object) {
        if (!object.isURI() && !object.isBlank()) {
            return;
        } else if (seenResources.contains(object)) {
            return;
        } else {
            resourceQueue.add(object);
            seenResources.add(object);
        }
    }

    @Override
    public boolean hasNext() {
        return !resourceQueue.isEmpty();
    }

    @Override
    public Node next() {
        return resourceQueue.remove();
    }

    @Override
    public void close() {
    }
}
