package cz.cuni.mff.odcleanstore.fusiontool;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import cz.cuni.mff.odcleanstore.fusiontool.testutil.ODCSFTTestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openrdf.model.URI;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ODCSFusionToolComponentFactoryTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void getsPreferredUris() throws Exception {
        Set<URI> settingsURIs = ImmutableSet.of(ODCSFTTestUtils.createHttpUri("s1"), ODCSFTTestUtils.createHttpUri("s2"));
        File canonicalUriFile = temporaryFolder.newFile();
        Files.write(canonicalUriFile.toPath(), ImmutableList.of("http://c1", "http://c2"), Charset.forName("UTF-8"));
        Set<String> preferredURIs = ImmutableSet.of(ODCSFTTestUtils.createHttpUri("p1").stringValue(), ODCSFTTestUtils.createHttpUri("p2").stringValue());

        Set<String> actualUris = ODCSFusionToolComponentFactory.getPreferredURIs(settingsURIs, canonicalUriFile, preferredURIs);

        Set<String> expectedUris = ImmutableSet.of(
                ODCSFTTestUtils.createHttpUri("s1").stringValue(),
                ODCSFTTestUtils.createHttpUri("s2").stringValue(),
                "http://c1",
                "http://c2",
                ODCSFTTestUtils.createHttpUri("p1").stringValue(),
                ODCSFTTestUtils.createHttpUri("p2").stringValue());
        assertThat(actualUris, is(expectedUris));
    }

    @Test
    public void getsPreferredUrisWithoutFile() throws Exception {
        Set<URI> settingsURIs = ImmutableSet.of(ODCSFTTestUtils.createHttpUri("s1"), ODCSFTTestUtils.createHttpUri("s2"));
        Set<String> preferredURIs = ImmutableSet.of(ODCSFTTestUtils.createHttpUri("p1").stringValue(), ODCSFTTestUtils.createHttpUri("p2").stringValue());

        Set<String> actualUris = ODCSFusionToolComponentFactory.getPreferredURIs(settingsURIs, null, preferredURIs);

        Set<String> expectedUris = ImmutableSet.of(
                ODCSFTTestUtils.createHttpUri("s1").stringValue(),
                ODCSFTTestUtils.createHttpUri("s2").stringValue(),
                ODCSFTTestUtils.createHttpUri("p1").stringValue(),
                ODCSFTTestUtils.createHttpUri("p2").stringValue());
        assertThat(actualUris, is(expectedUris));
    }
}