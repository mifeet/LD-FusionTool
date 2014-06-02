package cz.cuni.mff.odcleanstore.fusiontool.io.ntuples;

import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFParseException;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class NTuplesParserUtilsTest {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    @Test
    public void parseValidResourceParsesUri() throws Exception {
        assertThat(NTuplesParserUtils.parseValidResource("<http://abc>"), is((Resource) VF.createURI("http://abc")));
        assertThat(NTuplesParserUtils.parseValidResource("<http://abc>\nxxx"), is((Resource) VF.createURI("http://abc")));
        assertThat(NTuplesParserUtils.parseValidResource("<http://abc>."), is((Resource) VF.createURI("http://abc")));
    }

    @Test
    public void parseValidResourceParsesBlankNode() throws Exception {
        assertThat(NTuplesParserUtils.parseValidResource("_:abc"), is((Resource) VF.createBNode("abc")));
        assertThat(NTuplesParserUtils.parseValidResource("_:a_b-c "), is((Resource) VF.createBNode("a_b-c")));
        assertThat(NTuplesParserUtils.parseValidResource("_:abc.."), is((Resource) VF.createBNode("abc")));
    }

    @Test(expected = RDFParseException.class)
    public void parseValidResourceThrowsWhenUriIsEmpty() throws Exception {
        NTuplesParserUtils.parseValidResource("<>");
    }

    @Test(expected = RDFParseException.class)
    public void parseValidResourceThrowsWhenBlankNodeIsEmpty() throws Exception {
        NTuplesParserUtils.parseValidResource("_:");
    }

    @Test(expected = RDFParseException.class)
    public void parseValidResourceThrowsWhenNotAResource() throws Exception {
        NTuplesParserUtils.parseValidResource("\"a\"");
    }
}