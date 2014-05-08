package cz.cuni.mff.odcleanstore.fusiontool.io;

import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class SplitFileNameGeneratorTest {

    private File directory;

    @Before
    public void setUp() throws Exception {
        directory = new File(getClass().getResource("/").toURI());
    }

    @Test
    public void generatesNamesWhenFileWithExtensionGiven() throws Exception {
        // Arrange
        File file = new File(directory, "abc.txt");
        String pathPrefix = directory.getAbsolutePath() + File.separatorChar;

        // Act && assert
        SplitFileNameGenerator generator = new SplitFileNameGenerator(file);
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-1.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-2.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-3.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-4.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-5.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-6.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-7.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-8.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-9.txt"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-10.txt"));
    }

    @Test
    public void generatesNamesWhenFileWithoutExtensionGiven() throws Exception {
        // Arrange
        File file = new File(directory, "abc");
        String pathPrefix = directory.getAbsolutePath() + File.separatorChar;

        // Act && assert
        SplitFileNameGenerator generator = new SplitFileNameGenerator(file);
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-1"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-2"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-3"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-4"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-5"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-6"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-7"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-8"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-9"));
        assertThat(generator.nextFile().getAbsolutePath(), equalTo(pathPrefix + "abc-10"));
    }
}