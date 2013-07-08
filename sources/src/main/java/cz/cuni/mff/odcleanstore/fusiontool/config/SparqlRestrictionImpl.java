/**
 * 
 */
package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Implementation of {@link SparqlRestriction}.
 * @author Jan Michelfeit
 */
public class SparqlRestrictionImpl implements SparqlRestriction {
    private final String pattern;
    private final String var;
    
    /**
     * @param pattern value for {@link #getPattern()}
     * @param var value for {@link #getVar()}
     */
    public SparqlRestrictionImpl(String pattern, String var) {
        this.pattern = pattern;
        this.var = var;
    }
    
    @Override
    public String getPattern() {
        return pattern; 
    }

    @Override
    public String getVar() {
        return var;
    }

}
