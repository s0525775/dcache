package diskCacheV111.poolManager;

import diskCacheV111.poolManager.PoolSelectionUnit.SelectionLink;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPoolGroup;
import diskCacheV111.pools.PoolV2Mode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class Pool extends PoolCore implements SelectionPool {
    static final long serialVersionUID = 8108406418388363116L;
    final Map<String, PGroup> _pGroupList = new HashMap<String, PGroup>();
    boolean _enabled = true;
    private long _active = 0L;
    private boolean _ping = true;
    private long _serialId = 0L;
    boolean _rdOnly = false;
    private Set<String> _hsmInstances = new HashSet<String>(0);
    private PoolV2Mode _mode = new PoolV2Mode(PoolV2Mode.DISABLED);

    Pool(String name) {
        super(name);
    }

    @Override
    public Collection<SelectionLink> getLinksTargetingPool() {
        return new ArrayList<SelectionLink>(_linkList.values());
    }

    @Override
    public Collection<SelectionPoolGroup> getPoolGroupsMemberOf() {
        return new ArrayList<SelectionPoolGroup>(_pGroupList.values());
    }

    @Override
    public void setActive(boolean active) {
        _active = active ? System.currentTimeMillis() : 0;
    }

    @Override
    public long getActive() {
        return _ping ? (System.currentTimeMillis() - _active) : 0L;
    }

    /**
     * Returns true if pool heartbeat was received within the last
     * 5 minutes.
     */
    @Override
    public boolean isActive() {
        return getActive() < 5 * 60 * 1000;
    }

    @Override
    public void setReadOnly(boolean rdOnly) {
        _rdOnly = rdOnly;
    }

    @Override
    public boolean isReadOnly() {
        return _rdOnly;
    }

    /**
     * Returns true if reading from the pool is allowed.
     */
    @Override
    public boolean canRead() {
        return isEnabled() && _mode.getMode() != PoolV2Mode.DISABLED && !_mode.isDisabled(PoolV2Mode.DISABLED_FETCH) && !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
    }

    /**
     * Returns true if writing to the pool is allowed. Since we
     * cannot distinguish between a client write and a
     * pool-to-pool write, both operations must be enabled on the
     * pool.
     */
    @Override
    public boolean canWrite() {
        return isEnabled() && !isReadOnly() && _mode.getMode() != PoolV2Mode.DISABLED && !_mode.isDisabled(PoolV2Mode.DISABLED_STORE) && !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD) && !_mode.isDisabled(PoolV2Mode.DISABLED_P2P_SERVER);
    }

    /**
     * Returns true if the pool is allowed to read from tape.
     */
    @Override
    public boolean canReadFromTape() {
        return isEnabled() && !isReadOnly() && _mode.getMode() != PoolV2Mode.DISABLED && !_mode.isDisabled(PoolV2Mode.DISABLED_STAGE) && !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
    }

    /**
     * Returns true if the pool can deliver files for P2P
     * operations.
     */
    @Override
    public boolean canReadForP2P() {
        return isEnabled() && _mode.getMode() != PoolV2Mode.DISABLED && !_mode.isDisabled(PoolV2Mode.DISABLED_P2P_SERVER) && !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
    }

    /**
     * Returns true if the pool can receive files for P2P
     * operations.
     */
    public boolean canWriteForP2P() {
        return isEnabled() && !isReadOnly() && _mode.getMode() != PoolV2Mode.DISABLED && !_mode.isDisabled(PoolV2Mode.DISABLED_P2P_CLIENT) && !_mode.isDisabled(PoolV2Mode.DISABLED_DEAD);
    }

    public void setEnabled(boolean enabled) {
        _enabled = enabled;
    }

    @Override
    public boolean isEnabled() {
        return _enabled;
    }

    public void setPing(boolean ping) {
        _ping = ping;
    }

    public boolean isPing() {
        return _ping;
    }

    @Override
    public String toString() {
        return _name + "  (enabled=" + _enabled + ";active=" + (_active > 0 ? (getActive() / 1000) : "no") + ";rdOnly=" + isReadOnly() + ";links=" + _linkList.size() + ";pgroups=" + _pGroupList.size() + ";hsm=" + _hsmInstances.toString() + ";mode=" + _mode + ")";
    }

    @Override
    public boolean setSerialId(long serialId) {
        if (serialId == _serialId) {
            return false;
        }
        _serialId = serialId;
        return true;
    }

    @Override
    public void setPoolMode(PoolV2Mode mode) {
        _mode = mode;
    }

    @Override
    public PoolV2Mode getPoolMode() {
        return _mode;
    }

    @Override
    public Set<String> getHsmInstances() {
        return _hsmInstances;
    }

    @Override
    public void setHsmInstances(Set<String> hsmInstances) {
        if (hsmInstances == null) {
            hsmInstances = new HashSet<String>(0);
        }
        _hsmInstances = hsmInstances;
    }

}