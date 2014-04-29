package cz.cuni.mff.odcleanstore.fusiontool.io;

import java.io.IOException;

import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpState;
import org.apache.commons.httpclient.ProxyHost;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.openrdf.http.client.HTTPClient;
import org.openrdf.repository.sparql.SPARQLRepository;

/**
 * SPARQL repository which issues at most one query per the given time interval 
 * so that the SPARQL source endpoint is not overloaded.
 * @author Jan Michelfeit
 */
public class WellBehavedSPARQLRepository extends SPARQLRepository {
    private long minQueryIntervalMs;
    
    /**
     * @param endpointUrl URL of SPARQL endpoint to access
     * @param minQueryIntervalMs minimal interval between queries in milliseconds
     */
    public WellBehavedSPARQLRepository(String endpointUrl, long minQueryIntervalMs) {
        super(endpointUrl);
        this.minQueryIntervalMs = minQueryIntervalMs;
    }
    
    /**
     * @param queryEndpointUrl URL of SPARQL endpoint to access for queries
     * @param updateEndpointUrl URL of SPARQL-update capable endpoint
     * @param minQueryIntervalMs minimal interval between queries in milliseconds
     */
    public WellBehavedSPARQLRepository(String queryEndpointUrl, String updateEndpointUrl, long minQueryIntervalMs) {
        super(queryEndpointUrl, updateEndpointUrl);
        this.minQueryIntervalMs = minQueryIntervalMs;
    }

    @Override
    protected HTTPClient createHTTPClient() {
        return new WellBehavedOpenRdfHTTPClient(); 
    }
    
    /**
     * Implementation of {@link HTTPClient} limiting the frequency of requests.
     */
    protected class WellBehavedOpenRdfHTTPClient extends HTTPClient {
        private static final int DEFAULT_PROXY_PORT = 80;

        @Override
        public void initialize() {
            if (httpClient == null) {
                super.initialize();
                httpClient = new WellBehavedHttpClient(httpClient.getHttpConnectionManager());
                configureProxySettings(httpClient);
            }
        }

        private void configureProxySettings(HttpClient httpClient) {
            String proxyHostName = System.getProperty("http.proxyHost");
            if (proxyHostName != null && proxyHostName.length() > 0) {
                int proxyPort = DEFAULT_PROXY_PORT; // default
                try {
                    proxyPort = Integer.parseInt(System.getProperty("http.proxyPort"));
                } catch (NumberFormatException e) {
                    // do nothing, revert to default
                }
                ProxyHost proxyHost = new ProxyHost(proxyHostName, proxyPort);
                httpClient.getHostConfiguration().setProxyHost(proxyHost);

                String proxyUser = System.getProperty("http.proxyUser");
                if (proxyUser != null) {
                    String proxyPassword = System.getProperty("http.proxyPassword");
                    httpClient.getState().setProxyCredentials(
                            new AuthScope(proxyHost.getHostName(), proxyHost.getPort()),
                            new UsernamePasswordCredentials(proxyUser, proxyPassword));
                    httpClient.getParams().setAuthenticationPreemptive(true);
                }
            }
        }
    }
    
    /**
     * Implementation of {@link HttpClient} limiting the frequency of requests.
     */
    protected class WellBehavedHttpClient extends HttpClient {
        private long lastQueryTime = 0;

        /**
         * @param httpConnectionManager The {@link HttpConnectionManager connection manager} to use.
         */
        public WellBehavedHttpClient(HttpConnectionManager httpConnectionManager) {
            super(httpConnectionManager);
        }

        @Override
        public int executeMethod(HostConfiguration hostconfig, HttpMethod method, HttpState state) throws IOException {
            long now = System.currentTimeMillis();
            long waitPeriod = minQueryIntervalMs - (now - lastQueryTime);
            lastQueryTime = now;
            if (waitPeriod > 0) {
                try {
                    Thread.sleep(waitPeriod);
                } catch (InterruptedException e) {
                    // nothing
                }
            }
            return super.executeMethod(hostconfig, method, state);
        }

        @Override
        public int executeMethod(HttpMethod method) throws IOException {
            return executeMethod(null, method, null);
        }

        @Override
        public int executeMethod(final HostConfiguration hostConfiguration, final HttpMethod method) throws IOException {
            return executeMethod(hostConfiguration, method, null);
        }
    }
}
