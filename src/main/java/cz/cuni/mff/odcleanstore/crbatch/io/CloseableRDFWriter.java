/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.io;


import com.hp.hpl.jena.rdf.model.Model;

import cz.cuni.mff.odcleanstore.crbatch.util.Closeable;

/**
 * TODO
 * @author Jan Michelfeit
 */
public interface CloseableRDFWriter extends Closeable {
      void write(Model model);
}
