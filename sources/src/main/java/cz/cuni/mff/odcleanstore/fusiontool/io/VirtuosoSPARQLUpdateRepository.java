/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.io;

import info.aduna.iteration.EmptyIteration;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.NameValuePair;
import org.openrdf.http.client.HTTPClient;
import org.openrdf.http.protocol.Protocol;
import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.Binding;
import org.openrdf.query.BooleanQuery;
import org.openrdf.query.Dataset;
import org.openrdf.query.GraphQuery;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.Query;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.Update;
import org.openrdf.query.UpdateExecutionException;
import org.openrdf.query.parser.sparql.SPARQLUtil;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.base.RepositoryConnectionBase;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.repository.sparql.query.SPARQLUpdate;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write-only SPARQL repository for connecting to Virtuoso SPARQL endpoint, handling its diversions from the standard.
 * @author Jan Michelfeit
 */
public class VirtuosoSPARQLUpdateRepository extends SPARQLRepository {
    private static final Logger LOG = LoggerFactory.getLogger(VirtuosoSPARQLUpdateRepository.class);

    private static final Resource[] DEFAULT_CONTEXTS = new Resource[] { null };
    private static final String UPDATE_PARAM_NAME = "query";

    /**
     * Create a new SPARQLREpository using the supplied query endpoint URL for
     * queries, and the supplied update endpoint URL for updates.
     * 
     * @param queryEndpointUrl
     *        a SPARQL endpoint URL for queries. May not be null.
     * @param updateEndpointUrl
     *        a SPARQL endpoint URL for updates. May not be null.
     */
    public VirtuosoSPARQLUpdateRepository(String queryEndpointUrl, String updateEndpointUrl) {
        super(queryEndpointUrl, updateEndpointUrl);
    }

    @Override
    public RepositoryConnection getConnection() throws RepositoryException {
        if (!isInitialized()) {
            throw new RepositoryException("SPARQLRepository not initialized.");
        }
        return new SPARQLUpdateConnection();
    }

    @Override
    protected HTTPClient createHTTPClient() {
        return new VirtuosoHttpClient();
    }

    /**
     * HTTP Client which overrides "update" request query variable with "query" variable (see {@link #UPDATE_PARAM_NAME}).
     * @author Jan Michelfeit
     */
    private class VirtuosoHttpClient extends HTTPClient {
        @Override
        protected List<NameValuePair> getUpdateMethodParameters(QueryLanguage ql, String update, String baseURI,
                Dataset dataset, boolean includeInferred, Binding... bindings) {
            List<NameValuePair> queryParams = new ArrayList<NameValuePair>(bindings.length + 1);

            queryParams.add(new NameValuePair(Protocol.QUERY_LANGUAGE_PARAM_NAME, ql.getName()));
            queryParams.add(new NameValuePair(UPDATE_PARAM_NAME, update));
            if (baseURI != null) {
                queryParams.add(new NameValuePair(Protocol.BASEURI_PARAM_NAME, baseURI));
            }
            queryParams.add(new NameValuePair(Protocol.INCLUDE_INFERRED_PARAM_NAME,
                    Boolean.toString(includeInferred)));

            if (dataset != null) {
                for (URI graphURI : dataset.getDefaultRemoveGraphs()) {
                    queryParams.add(new NameValuePair(Protocol.REMOVE_GRAPH_PARAM_NAME, String.valueOf(graphURI)));
                }
                if (dataset.getDefaultInsertGraph() != null) {
                    queryParams.add(new NameValuePair(Protocol.INSERT_GRAPH_PARAM_NAME,
                            String.valueOf(dataset.getDefaultInsertGraph())));
                }
                for (URI defaultGraphURI : dataset.getDefaultGraphs()) {
                    queryParams.add(new NameValuePair(Protocol.USING_GRAPH_PARAM_NAME,
                            String.valueOf(defaultGraphURI)));
                }
                for (URI namedGraphURI : dataset.getNamedGraphs()) {
                    queryParams.add(new NameValuePair(Protocol.USING_NAMED_GRAPH_PARAM_NAME,
                            String.valueOf(namedGraphURI)));
                }
            }

            for (int i = 0; i < bindings.length; i++) {
                String paramName = Protocol.BINDING_PREFIX + bindings[i].getName();
                String paramValue = Protocol.encodeValue(bindings[i].getValue());
                queryParams.add(new NameValuePair(paramName, paramValue));
            }

            return queryParams;
        }
    }

    /**
     * SPARQL connection using SPARQL UPDATE syntax rather than SPARQL 1.1.
     */
    private class SPARQLUpdateConnection extends RepositoryConnectionBase {
        private StringBuffer sparqlTransaction;
        private Object transactionLock = new Object();

        public SPARQLUpdateConnection() {
            super(VirtuosoSPARQLUpdateRepository.this);
        }

        @Override
        public String getNamespace(String prefix) throws RepositoryException {
            return null;
        }

        @Override
        public void setNamespace(String prefix, String name) throws RepositoryException {
        }

        @Override
        public void removeNamespace(String prefix) throws RepositoryException {
        }

        @Override
        public void clearNamespaces() throws RepositoryException {
        }

        @Override
        public RepositoryResult<Namespace> getNamespaces() throws RepositoryException {
            return new RepositoryResult<Namespace>(new EmptyIteration<Namespace, RepositoryException>());
        }

        @Override
        public void add(Statement st, Resource... contexts) throws RepositoryException {
            boolean localTransaction = startLocalTransaction();

            addWithoutCommit(st.getSubject(), st.getPredicate(), st.getObject(), contexts);

            try {
                conditionalCommit(localTransaction);
            } catch (RepositoryException e) {
                conditionalRollback(localTransaction);
                throw e;
            }
        }

        @Override
        public void begin() throws RepositoryException {
            synchronized (transactionLock) {
                if (!isActive()) {
                    synchronized (transactionLock) {
                        sparqlTransaction = new StringBuffer();
                    }
                } else {
                    throw new RepositoryException("active transaction already exists");
                }
            }
        }

        @Override
        public void commit() throws RepositoryException {
            try {
                commitInternal();
            } catch (RepositoryException e) {
                LOG.warn("SPARQL update error, some statements may not be written; error message: {}", e.getMessage());
            }
        }

        protected void commitInternal() throws RepositoryException {
            synchronized (transactionLock) {
                if (isActive()) {
                    synchronized (transactionLock) {
                        SPARQLUpdate transaction = new SPARQLUpdate(VirtuosoSPARQLUpdateRepository.this.getHTTPClient(), null,
                                sparqlTransaction.toString());
                        try {
                            transaction.execute();
                        } catch (UpdateExecutionException e) {
                            throw new RepositoryException("error executing transaction", e);
                        }

                        sparqlTransaction = null;
                    }
                } else {
                    throw new RepositoryException("no transaction active.");
                }
            }

        }

        @Override
        public void rollback() throws RepositoryException {
            synchronized (transactionLock) {
                if (isActive()) {
                    synchronized (transactionLock) {
                        sparqlTransaction = null;
                    }
                } else {
                    throw new RepositoryException("no transaction active.");
                }
            }
        }

        @Override
        public boolean isActive() throws RepositoryException {
            synchronized (transactionLock) {
                return sparqlTransaction != null;
            }
        }

        @Override
        protected void addWithoutCommit(Resource subject, URI predicate, Value object, Resource... contexts)
                throws RepositoryException {

            if (contexts.length == 0) {
                contexts = DEFAULT_CONTEXTS;
            }
            for (int i = 0; i < contexts.length; i++) {
                String sparqlCommand = createInsertDataCommand(subject, predicate, object, contexts[i]);
                if (sparqlTransaction.length() > 0) {
                    sparqlTransaction.append("; ");
                }
                sparqlTransaction.append(sparqlCommand);
            }
        }

        @Override
        public SPARQLRepository getRepository() {
            return (SPARQLRepository) super.getRepository();
        }

        private String createInsertDataCommand(Resource subject, URI predicate, Value object, Resource context) {
            StringBuilder qb = new StringBuilder();
            qb.append("INSERT DATA ");
            if (context != null) {
                qb.append("INTO <");
                String namedGraph = context.stringValue();
                if (context instanceof BNode) {
                    namedGraph = "urn:nodeid:" + context.stringValue();
                }
                qb.append(namedGraph);
                qb.append("> ");
            }
            qb.append(" {\n");
            createDataBody(qb, subject, predicate, object);
            qb.append("}");

            return qb.toString();
        }

        private void createDataBody(StringBuilder qb, Resource subject, URI predicate, Value object) {
            if (subject instanceof BNode) {
                qb.append("_:");
                qb.append(subject.stringValue());
                qb.append(" ");
            } else {
                qb.append("<");
                qb.append(subject.stringValue());
                qb.append("> ");
            }

            qb.append("<");
            qb.append(predicate.stringValue());
            qb.append("> ");

            if (object instanceof Literal) {
                Literal lit = (Literal) object;
                qb.append("\"");
                qb.append(SPARQLUtil.encodeString(lit.getLabel()));
                qb.append("\"");

                if (lit.getLanguage() != null) {
                    qb.append("@");
                    qb.append(lit.getLanguage());
                }

                if (lit.getDatatype() != null) {
                    qb.append("^^<");
                    qb.append(lit.getDatatype().stringValue());
                    qb.append(">");
                }
                qb.append(" ");
            } else if (object instanceof BNode) {
                qb.append("_:");
                qb.append(object.stringValue());
                qb.append(" ");
            } else {
                qb.append("<");
                qb.append(object.stringValue());
                qb.append("> ");
            }
            qb.append(". \n");
        }

        @Override
        public Query prepareQuery(QueryLanguage ql, String query, String baseURI) throws RepositoryException,
                MalformedQueryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        public TupleQuery prepareTupleQuery(QueryLanguage ql, String query, String baseURI) throws RepositoryException,
                MalformedQueryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        public GraphQuery prepareGraphQuery(QueryLanguage ql, String query, String baseURI) throws RepositoryException,
                MalformedQueryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        public BooleanQuery prepareBooleanQuery(QueryLanguage ql, String query, String baseURI) throws RepositoryException,
                MalformedQueryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        public Update prepareUpdate(QueryLanguage ql, String update, String baseURI) throws RepositoryException,
                MalformedQueryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        public RepositoryResult<Resource> getContextIDs() throws RepositoryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        public RepositoryResult<Statement> getStatements(Resource subj, URI pred, Value obj, boolean includeInferred,
                Resource... contexts) throws RepositoryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        public void exportStatements(Resource subj, URI pred, Value obj, boolean includeInferred, RDFHandler handler,
                Resource... contexts) throws RepositoryException, RDFHandlerException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        public long size(Resource... contexts) throws RepositoryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }

        @Override
        protected void removeWithoutCommit(Resource subject, URI predicate, Value object, Resource... contexts)
                throws RepositoryException {
            throw new UnsupportedOperationException("Not supported for SPARQL update endpoint");
        }
    }
}
