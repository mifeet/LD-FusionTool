/**
 * 
 */
package com.hp.hpl.jena.xmloutput.impl;

import com.hp.hpl.jena.vocabulary.RDF;

/**
 * @author Jan Michelfeit
 */
public abstract class JenaRdfXmlWriterTrojan extends Basic {
    /**
     * @return tab size
     */
    protected int getTabSize() {
        return tabSize;
    }

    /**
     * @return xml base
     */
    protected String getXmlBase() {
        return xmlBase;
    }

    /**
     * @return xmlnsDecl()
     */
    protected String jenaXmlnsDecl() {
        return super.xmlnsDecl();
    }

    /**
     * @param local 
     * @return rdfEl(local)
     */
    protected String jenaRdfEl(String local) {
        return super.rdfEl(local);
    }

    @Override
    void addNameSpace(String uri) {
        if (uri.equals(RDF.getURI())) {
            super.addNameSpace(uri);
        } else {
            // do nothing - avoid use of namespaces added in later dumps which are not declared in header, however
        }
    }
}
