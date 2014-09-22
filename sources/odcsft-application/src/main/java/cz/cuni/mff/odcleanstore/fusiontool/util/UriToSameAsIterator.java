package cz.cuni.mff.odcleanstore.fusiontool.util;

import cz.cuni.mff.odcleanstore.fusiontool.conflictresolution.urimapping.UriMappingIterable;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.vocabulary.OWL;

import java.util.Iterator;

public class UriToSameAsIterator extends ConvertingIterator<String, Statement> {
    private final UriMappingIterable uriMapping;
    private final ValueFactory valueFactory;

    public UriToSameAsIterator(Iterator<String> uriIterator, UriMappingIterable uriMapping, ValueFactory valueFactory) {
        super(uriIterator);
        this.uriMapping = uriMapping;
        this.valueFactory = valueFactory;
    }

    @Override
    public Statement convert(String uri) {
        String canonicalUri = uriMapping.getCanonicalURI(uri);
        return valueFactory.createStatement(
                valueFactory.createURI(uri),
                OWL.SAMEAS,
                valueFactory.createURI(canonicalUri));
    }
}
