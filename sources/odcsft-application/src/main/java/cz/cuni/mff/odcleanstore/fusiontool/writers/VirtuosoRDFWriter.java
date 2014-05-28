/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.writers;

import cz.cuni.mff.odcleanstore.connection.EnumLogLevel;
import cz.cuni.mff.odcleanstore.core.ODCSUtils;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolApplicationException;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolErrorCodes;
import cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException;
import cz.cuni.mff.odcleanstore.fusiontool.util.ODCSFusionToolApplicationUtils;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.parser.sparql.SPARQLUtil;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.helpers.RDFHandlerBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Locale;

/**
 * RDF writer using JDBC connection to Virtuoso.
 * @author Jan Michelfeit
 */
public class VirtuosoRDFWriter extends RDFHandlerBase implements RDFHandler, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(VirtuosoRDFWriter.class);
    private static final String DEFAULT_CONTEXT = "urn:odcs-fusion-tool:result";

    private Connection connection;
    private String name;

    /**
     * Creates an RDF Handler writing to Virtuoso database via JDBC.
     * @param name name of the RDF writer (for logging purposes)
     * @param host host for the connection
     * @param port connection port
     * @param username connection username
     * @param password connection password
     * @throws cz.cuni.mff.odcleanstore.fusiontool.exceptions.ODCSFusionToolException connection error
     */
    public VirtuosoRDFWriter(String name, String host, String port, String username, String password)
            throws ODCSFusionToolException {
        this.connection = createConnection(host, port, username, password);
        this.name = name;
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        executeQuery(createInsertQuery(
                statement.getSubject(),
                statement.getPredicate(),
                statement.getObject(),
                statement.getContext()));
    }

    private Connection createConnection(String host, String port, String username, String password)
            throws ODCSFusionToolException {

        try {
            Class.forName(ODCSUtils.JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_VIRTUOSO_JDBC,
                    "Couldn't load Virtuoso JDBC driver", e);
        }
        Connection connection;
        try {
            String connectionString = ODCSFusionToolApplicationUtils.getVirtuosoConnectionString(host, port);
            connection = DriverManager.getConnection(connectionString, username, password);

            // disable log by default in order to prevent log size problems; transactions don't work much with SPARQL anyway
            CallableStatement statement = connection.prepareCall(
                    String.format(Locale.ROOT, "log_enable(%d, 1)", EnumLogLevel.AUTOCOMMIT.getBits()));
            statement.execute();
            connection.setAutoCommit(EnumLogLevel.AUTOCOMMIT.getAutocommit());
        } catch (SQLException e) {
            throw new ODCSFusionToolApplicationException(ODCSFusionToolErrorCodes.REPOSITORY_INIT_VIRTUOSO_JDBC,
                    "Couldn't connect to Virtuoso via JDBC", e);
        }
        return connection;
    }

    private void executeQuery(String query) {
        try {
            java.sql.Statement statement = connection.createStatement();
            statement.execute(query);
        } catch (SQLException e) {
            LOG.warn("Virtuoso SPARQL update error, some statements may not be written to output {}\n    Error message: {}",
                    name, e.getMessage());
        }
    }

    private String createInsertQuery(Resource subject, URI predicate, Value object, Resource context) {
        StringBuilder query = new StringBuilder();
        query.append("SPARQL INSERT INTO <");
        if (context == null) {
            query.append(DEFAULT_CONTEXT);
        } else if (context instanceof BNode) {
            query.append("urn:nodeid:");
            query.append(context.stringValue());
        } else {
            query.append(context.stringValue());
        }
        query.append("> {\n");
        appendQuadPattern(query, subject, predicate, object);
        query.append("\n}");
        return query.toString();
    }

    private void appendQuadPattern(StringBuilder query, Resource subject, URI predicate, Value object) {
        if (subject instanceof BNode) {
            query.append("_:");
            query.append(subject.stringValue());
            query.append(" ");
        } else {
            query.append("<");
            query.append(subject.stringValue());
            query.append("> ");
        }

        query.append("<");
        query.append(predicate.stringValue());
        query.append("> ");

        if (object instanceof Literal) {
            Literal literal = (Literal) object;
            query.append("\"");
            query.append(SPARQLUtil.encodeString(literal.getLabel()));
            query.append("\"");

            if (literal.getLanguage() != null) {
                query.append("@");
                query.append(literal.getLanguage());
            }

            if (literal.getDatatype() != null) {
                query.append("^^<");
                query.append(literal.getDatatype().stringValue());
                query.append(">");
            }
            query.append(" ");
        } else if (object instanceof BNode) {
            query.append("_:");
            query.append(object.stringValue());
            query.append(" ");
        } else {
            query.append("<");
            query.append(object.stringValue());
            query.append("> ");
        }
        query.append(". \n");
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new IOException(e);
            }
            connection = null;
        }
    }
}
