package cz.cuni.mff.odcleanstore.fusiontool.config;

/**
 * Implementation of {@link cz.cuni.mff.odcleanstore.fusiontool.config.SeedResourceRestriction}.
 */
public class SeedResourceRestrictionImpl extends SparqlRestrictionImpl implements SeedResourceRestriction {
    private boolean isTransitive = true;

    /**
     * @param pattern value for {@link #getPattern()}
     * @param var value for {@link #getVar()}
     */
    public SeedResourceRestrictionImpl(String pattern, String var) {
        super(pattern, var);
    }

    @Override
    public boolean isTransitive() {
        return isTransitive;
    }

    /**
     * @param isTransitive value for {@link #isTransitive()}
     */
    public void setTransitive(boolean isTransitive) {
        this.isTransitive = isTransitive;
    }
}
