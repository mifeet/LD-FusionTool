package cz.cuni.mff.odcleanstore.fusiontool.testutil;

import cz.cuni.mff.odcleanstore.fusiontool.util.IsCanceledCallback;

public class TestIsCanceledCallback implements IsCanceledCallback {
    private boolean isCanceled = false;

    @Override
    public boolean isCanceled() {
        return isCanceled;
    }

    public void cancel() {
        this.isCanceled = true;
    }
}
