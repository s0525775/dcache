package org.dcache.pool.classic;

import java.util.Collections;
import java.util.List;

import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

/**
 * A ReplicaStatePolicy for volatile pools. Files are marked CACHED
 * and not sticky.
 */
public class VolatileReplicaStatePolicy implements ReplicaStatePolicy
{
    @Override
    public List<StickyRecord> getStickyRecords(FileAttributes fileAttributes)
    {
        return Collections.emptyList();
    }

    @Override
    public EntryState getTargetState(FileAttributes fileAttributes)
    {
        return EntryState.CACHED;
    }
}
