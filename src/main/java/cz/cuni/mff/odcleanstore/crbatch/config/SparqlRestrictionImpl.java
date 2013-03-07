/**
 * 
 */
package cz.cuni.mff.odcleanstore.crbatch.config;

/**
 * Implementation of {@link SparqlRestriction}.
 * @author Jan Michelfeit
 */
public class SparqlRestrictionImpl implements SparqlRestriction {
    private final String pattern;
    private final String var;
    
    /**
     * 
     */
    public SparqlRestrictionImpl() {
        this("", ConfigConstants.DEFAULT_RESTRICTION_VAR);
    }
    
    /**
     * @param pattern value for {@link #getPattern()}
     */
    public SparqlRestrictionImpl(String pattern) {
        this(pattern, ConfigConstants.DEFAULT_RESTRICTION_VAR);
    }
    
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
