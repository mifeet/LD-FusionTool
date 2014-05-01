package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.util.HashMap;
import java.util.Map;

import org.openrdf.model.Statement;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;

/**
 * An RDFHandler that adds RDF data to a repository.
 * 
 * @author Jan Michelfeit
 */
public class RepositoryRDFInserter extends RDFHandlerBase {
    private final RepositoryConnection connection;

    /**
     * Map that stores namespaces that are reported during the evaluation of the
     * query. Key is the namespace prefix, value is the namespace name.
     */
    private final Map<String, String> namespaceMap = new HashMap<String, String>();

    /**
     * Creates a new instance.
     * @param connection connection to use for add operations
     */
    public RepositoryRDFInserter(RepositoryConnection connection) {
        this.connection = connection;
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        for (Map.Entry<String, String> entry : namespaceMap.entrySet()) {
            String prefix = entry.getKey();
            String name = entry.getValue();
            try {
                if (connection.getNamespace(prefix) == null) {
                    connection.setNamespace(prefix, name);
                }
            } catch (RepositoryException e) {
                throw new RDFHandlerException(e);
            }
        }
        namespaceMap.clear();
    }

    @Override
    public void handleNamespace(String prefix, String name) {
        namespaceMap.put(prefix, name);
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        try {
            if (statement.getContext() != null) {
                connection.add(statement, statement.getContext());
            } else {
                connection.add(statement);
            }
        } catch (RepositoryException e) {
            throw new RDFHandlerException(e);
        }
    }
}
