package cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.impl;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.UriMapping;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.OWL;

import java.util.Collections;
import java.util.LinkedList;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UriMappingImplTest {
    private static final ValueFactory VF = ValueFactoryImpl.getInstance();

    @BeforeClass
    public static void setUpClass() throws Exception {
        ODCSFTTestUtils.resetURICounter();
    }

    private String getAndTestMappedUri(String uri, UriMapping mapping) {
        Resource mappedURI = mapping.mapResource(VF.createURI(uri));
        String mappedByNode = mappedURI.stringValue();
        String canonicalURI = mapping.getCanonicalURI(uri);
        Assert.assertEquals(mappedByNode, canonicalURI);
        return canonicalURI;
    }

    @Test
    public void testEmptyMapping() {
        UriMappingImpl instance = new UriMappingImpl();
        URI uri = VF.createURI(ODCSFTTestUtils.getUniqueURIString());
        URI result = instance.mapURI(uri);
        assertThat(result, is(uri));
    }

    @Test
    public void mapsBlankNodeWhenMapResourceCalled() throws Exception {
        // Arrange
        UriMappingImpl instance = new UriMappingImpl();
        String uriString1 = ODCSFTTestUtils.getUniqueURIString();
        String uriString2 = ODCSFTTestUtils.getUniqueURIString();
        instance.addLink(uriString1, uriString2);

        // Act
        BNode bNode1 = VF.createBNode(uriString1);
        Resource mapped1 = instance.mapResource(bNode1);
        BNode bNode2 = VF.createBNode(uriString2);
        Resource mapped2 = instance.mapResource(bNode2);

        // Assert
        assertThat(mapped1, is((Resource) bNode1));
        assertThat(mapped2, is((Resource) bNode2));
    }

    @Test
    public void testNonEmptyMapping1() {
        String uri1 = ODCSFTTestUtils.getUniqueURIString();
        String uri2 = ODCSFTTestUtils.getUniqueURIString();
        String uri3 = ODCSFTTestUtils.getUniqueURIString();
        String uri4 = ODCSFTTestUtils.getUniqueURIString();

        LinkedList<Statement> sameAsLinks = new LinkedList<Statement>();
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                uri1,
                OWL.SAMEAS.stringValue(),
                uri2));
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                uri2,
                OWL.SAMEAS.stringValue(),
                uri3));

        UriMappingImpl uriMapping = new UriMappingImpl();
        uriMapping.addLinks(sameAsLinks.iterator());

        String mappedURI1 = getAndTestMappedUri(uri1, uriMapping);
        String mappedURI2 = getAndTestMappedUri(uri2, uriMapping);
        String mappedURI3 = getAndTestMappedUri(uri3, uriMapping);
        String mappedURI4 = getAndTestMappedUri(uri4, uriMapping);

        Assert.assertEquals(mappedURI1, mappedURI2);
        Assert.assertEquals(mappedURI1, mappedURI3);
        Assert.assertFalse(mappedURI4.equals(mappedURI1));
    }

    @Test
    public void testNonEmptyMapping2() {
        String rootURI = ODCSFTTestUtils.getUniqueURIString();
        String uri1 = ODCSFTTestUtils.getUniqueURIString();
        String uri2 = ODCSFTTestUtils.getUniqueURIString();
        String uri3 = ODCSFTTestUtils.getUniqueURIString();

        LinkedList<Statement> sameAsLinks = new LinkedList<Statement>();
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                rootURI,
                OWL.SAMEAS.stringValue(),
                uri1));
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                rootURI,
                OWL.SAMEAS.stringValue(),
                uri2));
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                rootURI,
                OWL.SAMEAS.stringValue(),
                uri3));

        UriMappingImpl uriMapping = new UriMappingImpl();
        uriMapping.addLinks(sameAsLinks.iterator());

        String rootMappedURI = getAndTestMappedUri(rootURI, uriMapping);
        String mappedURI1 = getAndTestMappedUri(uri1, uriMapping);
        String mappedURI2 = getAndTestMappedUri(uri2, uriMapping);
        String mappedURI3 = getAndTestMappedUri(uri3, uriMapping);

        Assert.assertEquals(rootMappedURI, mappedURI1);
        Assert.assertEquals(rootMappedURI, mappedURI2);
        Assert.assertEquals(rootMappedURI, mappedURI3);
    }

    @Test
    public void testCycleMapping() {
        String uri1 = ODCSFTTestUtils.getUniqueURIString();
        String uri2 = ODCSFTTestUtils.getUniqueURIString();
        String uri3 = ODCSFTTestUtils.getUniqueURIString();

        LinkedList<Statement> sameAsLinks = new LinkedList<Statement>();
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                uri1,
                OWL.SAMEAS.stringValue(),
                uri2));
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                uri2,
                OWL.SAMEAS.stringValue(),
                uri3));
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                uri3,
                OWL.SAMEAS.stringValue(),
                uri1));

        UriMappingImpl uriMapping = new UriMappingImpl();
        uriMapping.addLinks(sameAsLinks.iterator());

        String mappedURI1 = getAndTestMappedUri(uri1, uriMapping);
        String mappedURI2 = getAndTestMappedUri(uri2, uriMapping);
        String mappedURI3 = getAndTestMappedUri(uri3, uriMapping);

        Assert.assertEquals(mappedURI1, mappedURI2);
        Assert.assertEquals(mappedURI1, mappedURI3);
    }

    @Test
    public void testPreferredURIs() {
        String uri1 = ODCSFTTestUtils.getUniqueURIString();
        String uri2 = ODCSFTTestUtils.getUniqueURIString();
        String uri3 = ODCSFTTestUtils.getUniqueURIString();

        LinkedList<Statement> sameAsLinks = new LinkedList<Statement>();
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                uri1,
                OWL.SAMEAS.stringValue(),
                uri2));
        sameAsLinks.add(ODCSFTTestUtils.createStatement(
                uri2,
                OWL.SAMEAS.stringValue(),
                uri3));

        String mappedURI1;

        UriMappingImpl mappingPreferring1 = new UriMappingImpl(Collections.singleton(uri1));
        mappingPreferring1.addLinks(sameAsLinks.iterator());
        mappedURI1 = getAndTestMappedUri(uri1, mappingPreferring1);
        Assert.assertEquals(uri1, mappedURI1);

        UriMappingImpl mappingPreferring2 = new UriMappingImpl(Collections.singleton(uri2));
        mappingPreferring2.addLinks(sameAsLinks.iterator());
        mappedURI1 = getAndTestMappedUri(uri1, mappingPreferring2);
        Assert.assertEquals(uri2, mappedURI1);

        UriMappingImpl mappingPreferring3 = new UriMappingImpl(Collections.singleton(uri3));
        mappingPreferring3.addLinks(sameAsLinks.iterator());
        mappedURI1 = getAndTestMappedUri(uri1, mappingPreferring3);
        Assert.assertEquals(uri3, mappedURI1);
    }
}

