package cz.cuni.mff.odcleanstore.fusiontool.io;

import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolUtils;
import org.openrdf.model.*;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

import java.util.HashMap;
import java.util.Map;

/**
 * An RDFHandler that adds RDF data to a repository.
 * Statement with non-null context is inserted as is, null context is replaced by default context.
 * @author Jan Michelfeit
 */
public class DefaultContextRDFInserter extends RDFHandlerBase {
    public final static boolean PRESERVE_BNODE_IDS = true;
    protected final RepositoryConnection connection;
    protected final Resource defaultContext;
    private final Map<String, String> namespaceMap;
    private final Map<String, BNode> bNodesMap;

    /**
     * @param connection connection for add operations
     * @param defaultContext default context for statements with null context
     */
    public DefaultContextRDFInserter(RepositoryConnection connection, URI defaultContext) {
        ODCSFusionToolUtils.checkNotNull(defaultContext);
        ODCSFusionToolUtils.checkNotNull(connection);
        this.connection = connection;
        this.defaultContext = defaultContext;
        namespaceMap = new HashMap<String, String>();
        bNodesMap = new HashMap<String, BNode>();
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        for (Map.Entry<String, String> entry : namespaceMap.entrySet()) {
            String prefix = entry.getKey();
            try {
                if (connection.getNamespace(prefix) == null) {
                    connection.setNamespace(prefix, entry.getValue());
                }
            } catch (RepositoryException e) {
                throw new RDFHandlerException(e);
            }
        }
        namespaceMap.clear();
        bNodesMap.clear();
    }

    @Override
    public void handleNamespace(String prefix, String name) {
        if (prefix != null && !namespaceMap.containsKey(prefix)) {
            namespaceMap.put(prefix, name);
        }
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        Resource subject = statement.getSubject();
        URI predicate = statement.getPredicate();
        Value object = statement.getObject();
        Resource context = statement.getContext() == null ? defaultContext : statement.getContext();

        if (!PRESERVE_BNODE_IDS) {
            if (subject instanceof BNode) {
                subject = mapBNode((BNode) subject);
            }
            if (object instanceof BNode) {
                object = mapBNode((BNode) object);
            }
            if (context instanceof BNode) {
                context = mapBNode((BNode) context);
            }
        }

        try {
            connection.add(subject, predicate, object, context);
        } catch (RepositoryException e) {
            throw new RDFHandlerException(e);
        }
    }

    private BNode mapBNode(BNode bNode) {
        BNode result = bNodesMap.get(bNode.getID());
        if (result == null) {
            result = connection.getRepository().getValueFactory().createBNode();
            bNodesMap.put(bNode.getID(), result);
        }
        return result;
    }
}
