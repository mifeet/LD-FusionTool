package cz.cuni.mff.odcleanstore.fusiontool.io;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class EnumSerializationFormatTest {
    @Test
    public void parsesRdfXml() throws Exception {
        assertThat(EnumSerializationFormat.parseFormat("rdfxml"), is(EnumSerializationFormat.RDF_XML));
        assertThat(EnumSerializationFormat.parseFormat("rdf/xml"), is(EnumSerializationFormat.RDF_XML));
        assertThat(EnumSerializationFormat.parseFormat("RDF/XML"), is(EnumSerializationFormat.RDF_XML));
    }

    @Test
    public void parsesN3() throws Exception {
        assertThat(EnumSerializationFormat.parseFormat("N3"), is(EnumSerializationFormat.N3));
        assertThat(EnumSerializationFormat.parseFormat("n3"), is(EnumSerializationFormat.N3));
        assertThat(EnumSerializationFormat.parseFormat("ntriples"), is(EnumSerializationFormat.N3));
        assertThat(EnumSerializationFormat.parseFormat("NTRIPLES"), is(EnumSerializationFormat.N3));
    }

    @Test
    public void parsesTrig() throws Exception {
        assertThat(EnumSerializationFormat.parseFormat("trig"), is(EnumSerializationFormat.TRIG));
        assertThat(EnumSerializationFormat.parseFormat("TriG"), is(EnumSerializationFormat.TRIG));
    }

    @Test
    public void parsesNquads() throws Exception {
        assertThat(EnumSerializationFormat.parseFormat("nquads"), is(EnumSerializationFormat.NQUADS));
        assertThat(EnumSerializationFormat.parseFormat("n-quads"), is(EnumSerializationFormat.NQUADS));
        assertThat(EnumSerializationFormat.parseFormat("NQuads"), is(EnumSerializationFormat.NQUADS));
    }

    @Test
    public void parsesHtml() throws Exception {
        assertThat(EnumSerializationFormat.parseFormat("html"), is(EnumSerializationFormat.HTML));
        assertThat(EnumSerializationFormat.parseFormat("HTML"), is(EnumSerializationFormat.HTML));
    }

    @Test
    public void returnsNullWhenParsingIncorrectFormat() throws Exception {
        assertThat(EnumSerializationFormat.parseFormat("rdf/txt"), nullValue());
    }
}
