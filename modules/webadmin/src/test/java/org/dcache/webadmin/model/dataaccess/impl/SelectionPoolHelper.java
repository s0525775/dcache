package org.dcache.webadmin.model.dataaccess.impl;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.pools.PoolV2Mode;
import java.util.Collections;
import java.util.Collection;
import java.util.Set;

/**
 *
 * @author jans
 */
public class SelectionPoolHelper implements SelectionPool {

    private boolean enabled = true;
    private boolean active = true;
    private boolean readonly = false;
    private String name = "";
    private PoolV2Mode mode =
            new PoolV2Mode(PoolV2Mode.ENABLED);

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public long getActive() {
        return 10000L;
    }

    @Override
    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public boolean isReadOnly() {
        return readonly;
    }

    @Override
    public void setReadOnly(boolean rdOnly) {
        this.readonly = rdOnly;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public boolean isEnabled() {
        return (mode.getMode() != PoolV2Mode.DISABLED_RDONLY
                && mode.getMode() != PoolV2Mode.DISABLED_STRICT);
    }

    @Override
    public boolean setSerialId(long serialId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public boolean isActive() {
        return active;
    }

    @Override
    public void setPoolMode(PoolV2Mode mode) {
        this.mode = mode;
    }

    @Override
    public PoolV2Mode getPoolMode() {
        return mode;
    }

    @Override
    public boolean canRead() {
        return true;
    }

    @Override
    public boolean canWrite() {
        return !readonly;
    }

    @Override
    public boolean canReadFromTape() {
        return false;
    }

    @Override
    public boolean canReadForP2P() {
        return false;
    }

    @Override
    public Set<String> getHsmInstances() {
        return Collections.EMPTY_SET;
    }

    @Override
    public void setHsmInstances(Set<String> hsmInstances) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Collection<SelectionPoolGroup> getPoolGroupsMemberOf() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public Collection<SelectionLink> getLinksTargetingPool() {
        return Collections.EMPTY_LIST;
    }
}