package org.dcache.tests.repository;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.io.*;

import org.junit.*;

import com.sleepycat.je.DatabaseException;
import diskCacheV111.repository.*;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.vehicles.*;
import org.dcache.vehicles.*;
import dmg.cells.nucleus.*;
import dmg.util.*;
import org.dcache.namespace.FileAttribute;
import org.dcache.tests.cells.CellAdapterHelper;
import org.dcache.tests.cells.CellStubHelper;
import org.dcache.tests.cells.Message;

import org.dcache.pool.repository.Account;
import org.dcache.pool.repository.v5.CacheRepositoryV5;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.Repository.OpenFlags;
import org.dcache.pool.repository.IllegalTransitionException;

import org.dcache.pool.classic.FairQueueAllocation;
import org.dcache.pool.classic.SpaceSweeper2;
import org.dcache.pool.repository.SpaceRecord;
import org.dcache.pool.repository.EntryState;
import static org.dcache.pool.repository.EntryState.*;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.AbstractStateChangeListener;
import org.dcache.pool.repository.StateChangeEvent;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.FlatFileStore;
import org.dcache.pool.repository.meta.file.FileMetaDataRepository;
import org.dcache.pool.repository.v3.RepositoryException;

public class RepositorySubsystemTest
    extends AbstractStateChangeListener
{
    private long size1 = 1024;
    private long size2 = 1024;
    private long size3 = 1024;
    private long size4 = 1024;
    private long size5 = 1024;

    private PnfsId id1; // Precious
    private PnfsId id2; // Cached
    private PnfsId id3; // Cached + Sticky
    private PnfsId id4; // Non existing entry
    private PnfsId id5; // Non existing entry

    private StorageInfo info1;
    private StorageInfo info2;
    private StorageInfo info3;
    private StorageInfo info4;
    private StorageInfo info5;

    private PnfsHandler pnfs;

    private Account account;
    private CacheRepositoryV5 repository;
    private SpaceSweeper2 sweeper;
    private MetaDataStore metaDataStore;

    private File root;
    private File metaDir;
    private File dataDir;

    private BlockingQueue<StateChangeEvent> stateChangeEvents =
        new LinkedBlockingQueue<StateChangeEvent>();

    private CellAdapterHelper cell;

    private void createFile(File file, long size)
        throws IOException
    {
        RandomAccessFile handle = new RandomAccessFile(file, "rw");
        try {
            for (long i = 0; i < size; i++)
                handle.writeByte(0);
        } finally {
            handle.close();
        }
    }

    private void createEntry(final PnfsId id,
                             final StorageInfo info,
                             final EntryState state,
                             final List<StickyRecord> sticky)
        throws Throwable
    {
        new CellStubHelper() {
            @Message(cell="pnfs")
            public Object message(PnfsSetFileAttributes msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws CacheException, IOException, InterruptedException
            {
                ReplicaDescriptor handle =
                    repository.createEntry(id,
                                           info,
                                           EntryState.FROM_CLIENT,
                                           state,
                                           sticky);
                try {
                    handle.allocate(info.getFileSize());
                    createFile(handle.getFile(), info.getFileSize());
                    handle.commit(null);
                } finally {
                    handle.close();
                }
            }
        };
    }

    private StorageInfo createStorageInfo(long size)
    {
        GenericStorageInfo info = new GenericStorageInfo();
        info.setFileSize(size);
        return info;
    }

    private void deleteDirectory(File dir)
    {
        File[] fileArray = dir.listFiles();

        if (fileArray != null)
        {
            for (int i = 0; i < fileArray.length; i++)
            {
                if (fileArray[i].isDirectory())
                    deleteDirectory(fileArray[i]);
                else
                    fileArray[i].delete();
            }
        }
        dir.delete();
    }

    private void initRepository()
        throws FileNotFoundException, DatabaseException
    {
        FairQueueAllocation allocator = new FairQueueAllocation();
        FileStore fileStore = new FlatFileStore(root);
        metaDataStore =
            new FileMetaDataRepository(fileStore, root);

        account = new Account();
        sweeper = new SpaceSweeper2();
        repository = new CacheRepositoryV5();

        allocator.setAccount(account);
        repository.setCellEndpoint(cell);
        repository.setAllocator(allocator);
        repository.setPnfsHandler(pnfs);
        repository.setAccount(account);
        repository.setMetaDataStore(metaDataStore);
        repository.setExecutor(Executors.newSingleThreadScheduledExecutor());
        repository.setSynchronousNotification(true);
        repository.addListener(this);
        repository.setSpaceSweeperPolicy(sweeper);
        repository.setMaxDiskSpace(5120);

        sweeper.setAccount(account);
        sweeper.setRepository(repository);
    }

    @Before
    public void setUp()
        throws Throwable
    {
        id1 = new PnfsId("000000000001");
        id2 = new PnfsId("000000000002");
        id3 = new PnfsId("000000000003");
        id4 = new PnfsId("000000000004");
        id5 = new PnfsId("000000000005");

        info1 = createStorageInfo(size1);
        info2 = createStorageInfo(size2);
        info3 = createStorageInfo(size3);
        info4 = createStorageInfo(0);
        info5 = createStorageInfo(size5);

        root = File.createTempFile("dtest", null);
        if (!root.delete())
            throw new IOException("Could not delete temp file");
        if (!root.mkdir())
            throw new IOException("Could not create temp dir");
        dataDir = new File(root, "data");
        metaDir = new File(root, "meta");

        if (!dataDir.mkdir())
            throw new IOException("Could not create data dir");
        if (!metaDir.mkdir())
            throw new IOException("Could not create meta dir");

        cell = new CellAdapterHelper("pool", "");
        pnfs = new PnfsHandler(new CellPath("pnfs"), "pool");
        pnfs.setCellEndpoint(cell);

        /* Create test data. Notice that the repository automatically
         * applies a short lived sticky record if we don't request
         * one. That is fine for normal operation, but for testing it
         * is not what we want. So we explicitly specify an expired
         * sticky record to avoid that the automatic sticky record is
         * created.
         */
        initRepository();
        repository.init();
        repository.load();
        createEntry(id1, info1, EntryState.PRECIOUS,
                    Arrays.asList(new StickyRecord("system", 0)));
        createEntry(id2, info2, EntryState.CACHED,
                    Arrays.asList(new StickyRecord("system", 0)));
        createEntry(id3, info3, EntryState.CACHED,
                    Arrays.asList(new StickyRecord("system", -1)));
        repository.shutdown();
        metaDataStore.close();

        /* Create repository.
         */
        initRepository();
    }

    @After
    public void tearDown()
        throws InterruptedException
    {
        repository.shutdown();
        metaDataStore.close();
        cell.die();
        if (root != null)
            deleteDirectory(root);
    }

    @Override
    public void stateChanged(StateChangeEvent event)
    {
        stateChangeEvents.add(event);
    }

    public void expectStateChangeEvent(PnfsId id, EntryState oldState, EntryState newState)
    {
        StateChangeEvent event = stateChangeEvents.poll();
        assertNotNull(event);
        assertEquals(id, event.getPnfsId());
        assertEquals(oldState, event.getOldState());
        assertEquals(newState, event.getNewState());
    }

    public void assertNoStateChangeEvent()
    {
        if (stateChangeEvents.size() > 0)
            fail("Unexpected state change event: "
                 + stateChangeEvents.remove());
    }

    private void assertSpaceRecord(long total, long free, long precious, long removable)
    {
        SpaceRecord space = repository.getSpaceRecord();
        assertEquals(total, space.getTotalSpace());
        assertEquals(free, space.getFreeSpace());
        assertEquals(precious, space.getPreciousSpace());
        assertEquals(removable, space.getRemovableSpace());
    }

    private void assertCacheEntry(CacheEntry entry, PnfsId id,
                                  long size, EntryState state)
    {
        assertEquals(id, entry.getPnfsId());
        assertEquals(size, entry.getReplicaSize());
        assertEquals(state, entry.getState());
    }

    private void assertCanOpen(PnfsId id, long size, EntryState state)
    {
        try {
            ReplicaDescriptor handle =
                repository.openEntry(id, EnumSet.noneOf(OpenFlags.class));
            try {
                assertEquals(new File(dataDir, id.toString()), handle.getFile());
                assertCacheEntry(handle.getEntry(), id, size, state);
            } finally {
                handle.close();
            }
        } catch (FileNotInCacheException e) {
            fail("Expected entry " + id + " not found");
        } catch (CacheException e) {
            fail("Failed to open " + id + ": " + e.getMessage());
        } catch (InterruptedException e) {
            fail("Failed to open " + id + ": " + e.getMessage());
        }
    }

    @Test(expected=IllegalStateException.class)
    public void testInitTwiceFails()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.init();
    }

    @Test(expected=IllegalStateException.class)
    public void testLoadTwiceFails()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        repository.load();
    }

    @Test(expected=IllegalStateException.class)
    public void testLoadBeforeInitFail()
        throws IOException, CacheException, InterruptedException
    {
        repository.load();
    }

    @Test(expected=IllegalStateException.class)
    public void testCreateEntryFailsBeforeLoad() throws Exception
    {
        repository.init();
        List<StickyRecord> stickyRecords = Collections.emptyList();
        repository.createEntry(id1, info1, FROM_CLIENT, PRECIOUS, stickyRecords);
    }

    @Test(expected=IllegalStateException.class)
    public void testOpenEntryFailsBeforeInit() throws Exception
    {
        repository.openEntry(id1, EnumSet.noneOf(OpenFlags.class));
    }

    @Test(expected=IllegalStateException.class)
    public void testGetEntryFailsBeforeInit() throws Exception
    {
        repository.getEntry(id1);
    }

    @Test(expected=IllegalStateException.class)
    public void testSetStickyFailsBeforeInit() throws Exception
    {
        repository.setSticky(id2, "system", 0, true);
    }

    @Test(expected=IllegalStateException.class)
    public void testGetStateFailsBeforeInit() throws Exception
    {
        repository.getState(id1);
    }

    @Test(expected=IllegalStateException.class)
    public void testSetStateFailsBeforeLoad() throws Exception
    {
        repository.init();
        repository.setState(id1, CACHED);
    }

    @Test
    public void testGetSpaceRecord()
        throws IOException, CacheException, InterruptedException
    {
        assertSpaceRecord(0, 0, 0, 0);
        repository.init();
        assertSpaceRecord(0, 0, 0, 0);
        repository.load();
        assertSpaceRecord(5120, 2048, 1024, 1024);
    }

    @Test
    public void testOpenEntryBeforeLoad()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        stateChangeEvents.clear();
        assertCanOpen(id1, size1, PRECIOUS);
        assertCanOpen(id2, size2, CACHED);
        assertCanOpen(id3, size3, CACHED);
    }

    @Test
    public void testOpenEntry()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();
        assertCanOpen(id1, size1, PRECIOUS);
        assertCanOpen(id2, size2, CACHED);
        assertCanOpen(id3, size3, CACHED);
    }

    @Test(expected=FileNotInCacheException.class)
    public void testOpenEntryFileNotFound()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();
        new CellStubHelper() {
            /* Attempting to open a non-existing entry triggers a
             * clear cache location message.
             */
            @Message(required=true,step=1,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws CacheException, InterruptedException
            {
                repository.openEntry(id4, EnumSet.noneOf(OpenFlags.class));
            }
        };
    }

    @Test
    public void testCreateEntryFromStore() throws Throwable {

        repository.init();
        repository.load();
        stateChangeEvents.clear();
        new CellStubHelper()  {

            @Message(required = true, step = 1, cell = "pnfs")
            public Object message(PnfsSetFileAttributes msg) {
                if( msg.getFileAttributes().isDefined(FileAttribute.SIZE) ) {
                    return new CacheException("");
                }
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                    throws CacheException, InterruptedException {
                List<StickyRecord> stickyRecords = Collections.emptyList();
                ReplicaDescriptor handle = repository.createEntry(id5, info5, FROM_STORE, CACHED, stickyRecords);
                try {
                    handle.allocate(info5.getFileSize());
                    createFile(handle.getFile(), info5.getFileSize());
                    handle.commit(null);
                }catch( IOException e) {
                    throw new CacheException(CacheException.ERROR_IO_DISK, e.getMessage());
                } finally {
                    handle.close();
                }
            }
        };
    }

    @Test
    public void testSetState()
        throws IOException, IllegalTransitionException,
               CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        assertCanOpen(id1, size1, PRECIOUS);
        repository.setState(id1, CACHED);
        expectStateChangeEvent(id1, PRECIOUS, CACHED);
        assertCanOpen(id1, size1, CACHED);

        repository.setState(id1, PRECIOUS);
        expectStateChangeEvent(id1, CACHED, PRECIOUS);
        assertCanOpen(id1, size1, PRECIOUS);
    }

    @Test(expected=IllegalTransitionException.class)
    public void testSetStateFileNotFound()
        throws IOException, IllegalTransitionException,
               CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        repository.setState(id4, CACHED);
    }

    @Test(expected=IllegalTransitionException.class)
    public void testSetStateToNew()
        throws IOException, IllegalTransitionException,
               CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        repository.setState(id1, NEW);
    }

    @Test(expected=IllegalTransitionException.class)
    public void testSetStateToDestroyed()
        throws IOException, IllegalTransitionException,
               CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        repository.setState(id1, DESTROYED);
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleClose()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        ReplicaDescriptor handle =
            repository.openEntry(id1, EnumSet.noneOf(OpenFlags.class));
        handle.close();
        handle.close();
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleGetFile()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        ReplicaDescriptor handle =
            repository.openEntry(id1, EnumSet.noneOf(OpenFlags.class));
        handle.close();
        handle.getFile();
    }

    @Test(expected=IllegalStateException.class)
    public void testClosedReadHandleGetEntry()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        ReplicaDescriptor handle =
            repository.openEntry(id1, EnumSet.noneOf(OpenFlags.class));
        handle.close();
        handle.getEntry();
    }

    @Test
    public void testSetSize()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        repository.setMaxDiskSpace(3072);
        assertSpaceRecord(3072, 0, 1024, 1024);
    }

    @Test(expected=IllegalArgumentException.class)
    public void testSetSizeNegative()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        repository.setMaxDiskSpace(-1);
    }

    @Test
    public void testRemoval()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        new CellStubHelper() {
            @Message(required=true,step=1,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws IllegalTransitionException,
                       CacheException, InterruptedException
            {
                repository.setState(id1, REMOVED);
                expectStateChangeEvent(id1, PRECIOUS, REMOVED);
                expectStateChangeEvent(id1, REMOVED, DESTROYED);
                assertStep("Cache location cleared", 1);
                assertEquals(repository.getState(id1), NEW);
            }
        };
    }

    @Test
    public void testRemoveWhileReading()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        new CellStubHelper() {
            @Message(required=true,step=0,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws CacheException, InterruptedException,
                       IllegalTransitionException
            {
                ReplicaDescriptor handle1 =
                    repository.openEntry(id1, EnumSet.noneOf(OpenFlags.class));
                repository.setState(id1, REMOVED);
                expectStateChangeEvent(id1, PRECIOUS, REMOVED);
                assertNoStateChangeEvent();
                assertStep("Cache location cleared", 1);
                handle1.close();
                expectStateChangeEvent(id1, REMOVED, DESTROYED);
            }
        };
    }

    /**
     * Removing a file while it is open will mark it removed, but as
     * long as the file has not been destroyed yet, the file may be
     * opened again.
     */
    @Test
    public void testRemoveOpenAgain()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        new CellStubHelper() {
            @Message(required=true,step=1,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            protected void run()
                throws CacheException, InterruptedException,
                       IllegalTransitionException
            {
                ReplicaDescriptor h1 =
                    repository.openEntry(id1, EnumSet.noneOf(OpenFlags.class));
                repository.setState(id1, REMOVED);
                expectStateChangeEvent(id1, PRECIOUS, REMOVED);
                assertStep("Cache location should have been cleared", 1);
                ReplicaDescriptor h2 =
                    repository.openEntry(id1, EnumSet.noneOf(OpenFlags.class));
                h1.close();
                assertNoStateChangeEvent();
                h2.close();
                expectStateChangeEvent(id1, REMOVED, DESTROYED);
            }
        };
    }

    @Test(expected=FileInCacheException.class)
    public void testCreateEntryFileExists()
        throws IOException, CacheException,
               InterruptedException, FileInCacheException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        List<StickyRecord> stickyRecords = Collections.emptyList();
        repository.createEntry(id1, info1, FROM_CLIENT, PRECIOUS, stickyRecords);
    }

    /* Helper method for creating a fourth entry in the repository.
     */
    private void createEntry4(final long overallocation,
                              final boolean failSetAttributes,
                              final boolean cancel,
                              final EntryState transferState,
                              final EntryState finalState)
        throws Throwable
    {
        new CellStubHelper() {
            boolean setAttr = false;
            boolean addCache = false;

            /* If files get garbage collected, then locations are
             * cleared.
             */
            @Message(required=false,step=1,cell="pnfs")
            public Object message(PnfsClearCacheLocationMessage msg)
            {
                msg.setSucceeded();
                return msg;
            }

            /* In case of commit.
             */
            @Message(required=false,step=3,cell="pnfs")
            public Object message(PnfsSetFileAttributes msg)
            {
                assertEquals(size4, msg.getFileAttributes().getSize());
                if (failSetAttributes) {
                    msg.setFailed(1, null);
                } else {
                    msg.setSucceeded();
                }
                setAttr = true;
                return msg;
            }

            /* In case we cancel or fail to commit.
             */
            @Message(required=false,step=5,cell="pnfs")
            public Object message2(PnfsAddCacheLocationMessage msg)
            {
                assertTrue(failSetAttributes || cancel);
                msg.setSucceeded();
                addCache = true;
                return msg;
            }

            protected void run()
                throws FileInCacheException,
                       CacheException,
                       InterruptedException,
                       IOException
            {
                List<StickyRecord> stickyRecords = Collections.emptyList();
                ReplicaDescriptor handle =
                    repository.createEntry(id4, info4, transferState,
                                           finalState, stickyRecords);
                try {
                    handle.allocate(size4 + overallocation);
                    assertStep("No clear after this point", 2);
                    createFile(handle.getFile(), size4);
                    if (!cancel)
                        handle.commit(null);
                } finally {
                    assertStep("No set attributes after this point", 4);
                    handle.close();
                }
                assertEquals("SetFileAttributes must be sent unless we don't try to commit",
                             !cancel, setAttr);
                assertEquals("AddCacheLocation must be sent if not committd ",
                             cancel || failSetAttributes, addCache);
            }
        };
    }


    @Test
    public void testCreateEntry()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        createEntry4(0, false, false, FROM_CLIENT, PRECIOUS);
        assertCanOpen(id4, size4, PRECIOUS);
        assertSpaceRecord(5120, 1024, 2048, 1024);
    }

    @Test(expected=CacheException.class)
    public void testCreateEntrySetAttributesFailed()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        try {
            createEntry4(100, true, false, FROM_CLIENT, PRECIOUS);
        } finally {
            assertCacheEntry(repository.getEntry(id4), id4, size4, BROKEN);
            assertSpaceRecord(5120, 1024, 1024, 1024);
        }
        // TODO: Check notification
    }

    @Test
    public void testCreateEntryUnderallocation()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        createEntry4(-100, false, false, FROM_CLIENT, PRECIOUS);
        assertCanOpen(id4, size4, PRECIOUS);
        assertSpaceRecord(5120, 1024, 2048, 1024);
        // TODO: Check notification
    }

    @Test
    public void testCreateEntryOverallocation()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        createEntry4(100, false, false, FROM_CLIENT, PRECIOUS);
        assertCanOpen(id4, size4, PRECIOUS);
        assertSpaceRecord(5120, 1024, 2048, 1024);
        // TODO: Check notification
    }

    @Test
    public void testCreateEntryOverallocationFail()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        createEntry4(100, false, true, FROM_CLIENT, PRECIOUS);
        assertCacheEntry(repository.getEntry(id4), id4, size4, BROKEN);
        assertSpaceRecord(5120, 1024, 1024, 1024);
        // TODO: Check notification
    }

    @Test(expected=IllegalArgumentException.class)
    public void testCreateEntryNegativeAllocation()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        ReplicaDescriptor handle =
            repository.createEntry(id4, info4, FROM_CLIENT, PRECIOUS, null);
        handle.allocate(-1);
    }

    @Test
    public void testCreateEntryOutOfSpace()
        throws Throwable
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        repository.setMaxDiskSpace(3072);
        createEntry4(0, false, false, FROM_CLIENT, PRECIOUS);
        assertCanOpen(id4, size4, PRECIOUS);
        assertSpaceRecord(3072, 0, 2048, 0);
        // TODO: Check notification
    }


    // See http://rt.dcache.org/Ticket/Display.html?id=7337
    @Ignore("Time-critical test; may fail under extreme load")
    @Test
    public void testStickyExpiration()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        long now = System.currentTimeMillis();
        assertFalse(repository.getEntry(id2).isSticky());
        repository.setSticky(id2, "system", now + 500, true);
        assertTrue(repository.getEntry(id2).isSticky());
        Thread.currentThread().sleep(600 + CacheRepositoryV5.EXPIRATION_CLOCKSHIFT_EXTRA_TIME);
        assertFalse(repository.getEntry(id2).isSticky());
    }

    @Test
    public void testStickyClear()
        throws IOException, CacheException, InterruptedException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        long now = System.currentTimeMillis();

        assertFalse(repository.getEntry(id2).isSticky());
        repository.setSticky(id2, "system", now + 500, true);
        assertTrue(repository.getEntry(id2).isSticky());
        repository.setSticky(id2, "system", 0, true);
        assertFalse(repository.getEntry(id2).isSticky());
    }

    @Test
    public void testDoubleAccountingOnCache()
        throws IOException, CacheException,
               InterruptedException, IllegalTransitionException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        assertSpaceRecord(5120, 2048, 1024, 1024);
        repository.setState(id1, CACHED);
        assertSpaceRecord(5120, 2048, 0, 2048);
        repository.setState(id1, CACHED);
        assertSpaceRecord(5120, 2048, 0, 2048);
    }

    @Test
    public void testDoubleAccountingOnPrecious()
        throws IOException, CacheException,
               InterruptedException, IllegalTransitionException
    {
        repository.init();
        repository.load();
        stateChangeEvents.clear();

        assertSpaceRecord(5120, 2048, 1024, 1024);
        repository.setState(id2, PRECIOUS);
        assertSpaceRecord(5120, 2048, 2048, 0);
        repository.setState(id2, PRECIOUS);
        assertSpaceRecord(5120, 2048, 2048, 0);
    }
}

