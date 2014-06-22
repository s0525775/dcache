package org.dcache.cdmi.door;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.AccessController;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.PingMoversTask;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import dmg.cells.nucleus.CellLifeCycleAware;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.*;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * This ResourceFactory exposes the dCache name space.
 */
public class CdmiDoor
    extends AbstractCellComponent
    implements CellMessageReceiver, CellLifeCycleAware
{
    private static final Logger _log =
        LoggerFactory.getLogger(CdmiDoor.class);

    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES =
        EnumSet.of(TYPE, PNFSID, CREATION_TIME, MODIFICATION_TIME, SIZE,
                   MODE, OWNER, OWNER_GROUP);

    private static final String PROTOCOL_INFO_NAME = "Http";
    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;
    private static final int PROTOCOL_INFO_UNKNOWN_PORT = 0;

    private static final long PING_DELAY = 300000;

    /**
     * In progress transfers. The key of the map is the session
     * id of the transfer.
     *
     * Note that the session id is cast to an integer - this is
     * because HttpProtocolInfo uses integer ids. Casting the
     * session ID increases the risk of collision due to wrapping
     * of the ID. However this can only happen if transfers are
     * longer than 50 days.
     */
    private final Map<Integer,HttpTransfer> _transfers =
        Maps.newConcurrentMap();

    private ScheduledExecutorService _executor;

    private int _moverTimeout = 180000;
    private TimeUnit _moverTimeoutUnit = MILLISECONDS;
    private long _killTimeout = 1500;
    private TimeUnit _killTimeoutUnit = MILLISECONDS;
    private long _transferConfirmationTimeout = 60000;
    private TimeUnit _transferConfirmationTimeoutUnit = MILLISECONDS;
    private int _bufferSize = 65536;
    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;
    private PnfsHandler _pnfs;
    private String _ioQueue;
    private InetAddress _internalAddress;
    private boolean _doRedirectOnRead = true;
    private boolean _doRedirectOnWrite = true;
    private boolean _isOverwriteAllowed;
    private boolean _isAnonymousListingAllowed;

    private TransferRetryPolicy _retryPolicy =
        TransferRetryPolicies.tryOncePolicy(_moverTimeout, _moverTimeoutUnit);

    public CdmiDoor()
        throws UnknownHostException
    {
        _internalAddress = InetAddress.getLocalHost();
    }

    /**
     * Returns the kill timeout in milliseconds.
     * @return
     */
    public long getKillTimeout()
    {
        return _killTimeout;
    }

    /**
     * The kill timeout is the time we wait for a transfer to
     * terminate after we killed the mover.
     *
     * @param timeout The mover timeout in milliseconds
     */
    public void setKillTimeout(long timeout)
    {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        _killTimeout = timeout;
    }

    public void setKillTimeoutUnit(TimeUnit unit)
    {
        _killTimeoutUnit = checkNotNull(unit);
    }

    public TimeUnit getKillTimeoutUnit()
    {
        return _killTimeoutUnit;
    }

    /**
     * Returns the mover timeout in milliseconds.
     * @return
     */
    public int getMoverTimeout()
    {
        return _moverTimeout;
    }

    /**
     * The mover timeout is the time we wait for the mover to start
     * after having been enqueued.
     *
     * @param timeout The mover timeout in milliseconds
     */
    public void setMoverTimeout(int timeout)
    {
        if (timeout <= 0) {
            throw new IllegalArgumentException("Timeout must be positive");
        }
        _moverTimeout = timeout;
        _retryPolicy = TransferRetryPolicies.tryOncePolicy(_moverTimeout, _moverTimeoutUnit);
    }

    public void setMoverTimeoutUnit(TimeUnit unit)
    {
        _moverTimeoutUnit = checkNotNull(unit);
        _retryPolicy = TransferRetryPolicies.tryOncePolicy(_moverTimeout, _moverTimeoutUnit);
    }

    public TimeUnit getMoverTimeoutUnit()
    {
        return _moverTimeoutUnit;
    }

    /**
     * Returns the transfer confirmation timeout in milliseconds.
     * @return
     */
    public long getTransferConfirmationTimeout()
    {
        return _transferConfirmationTimeout;
    }

    /**
     * The transfer confirmation timeout is the time we wait after we
     * know that an upload has finished and until we received the
     * transfer confirmation message from the pool.
     *
     * @param timeout The transfer confirmation timeout in milliseconds
     */
    public void setTransferConfirmationTimeout(long timeout)
    {
        _transferConfirmationTimeout = timeout;
    }

    public void setTransferConfirmationTimeoutUnit(TimeUnit unit)
    {
        _transferConfirmationTimeoutUnit = checkNotNull(unit);
    }

    public TimeUnit getTransferConfirmationTimeoutUnit()
    {
        return _transferConfirmationTimeoutUnit;
    }

    /**
     * Returns the buffer size in bytes.
     * @return
     */
    public int getBufferSize()
    {
        return _bufferSize;
    }

    /**
     * Sets the size of the buffer used when proxying uploads.
     *
     * @param bufferSize The buffer size in bytes
     */
    public void setBufferSize(int bufferSize)
    {
        _bufferSize = bufferSize;
    }

    /**
     * Return the pool IO queue to use for WebDAV transfers.
     * @return
     */
    public String getIoQueue()
    {
        return (_ioQueue == null) ? "" : _ioQueue;
    }

    /**
     * Sets the pool IO queue to use for WebDAV transfers.
     * @param ioQueue
     */
    public void setIoQueue(String ioQueue)
    {
        _ioQueue = (ioQueue != null && !ioQueue.isEmpty()) ? ioQueue : null;
    }

    /**
     * Sets whether read requests are redirected to the pool. If not,
     * then the door will act as a proxy.
     * @param redirect
     */
    public void setRedirectOnReadEnabled(boolean redirect)
    {
        _doRedirectOnRead = redirect;
    }

    public boolean isRedirectOnReadEnabled()
    {
        return _doRedirectOnRead;
    }

    public void setRedirectOnWriteEnabled(boolean redirect)
    {
        _doRedirectOnWrite = redirect;
    }

    public boolean isRedirectOnWriteEnabled()
    {
        return _doRedirectOnWrite;
    }

    public void setAnonymousListing(boolean isAllowed)
    {
        _isAnonymousListingAllowed = isAllowed;
    }

    public boolean isAnonymousListing()
    {
        return _isAnonymousListingAllowed;
    }

    /**
     * Sets the cell stub for PnfsManager communication.
     * @param stub
     */
    public void setPnfsStub(CellStub stub)
    {
        _pnfs = new PnfsHandler(checkNotNull(stub));
    }

    /**
     * Sets the cell stub for pool communication.
     * @param stub
     */
    public void setPoolStub(CellStub stub)
    {
        _poolStub = checkNotNull(stub);
    }

    /**
     * Sets the cell stub for PoolManager communication.
     * @param stub
     */
    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = checkNotNull(stub);
    }

    /**
     * Sets the cell stub for billing communication.
     * @param stub
     */
    public void setBillingStub(CellStub stub)
    {
        _billingStub = checkNotNull(stub);
    }

    /**
     * Sets the ScheduledExecutorService used for periodic tasks.
     * @param executor
     */
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
        _executor.scheduleAtFixedRate(new PingMoversTask<>(_transfers.values()),
                                      PING_DELAY, PING_DELAY,
                                      MILLISECONDS);
    }

    public void setInternalAddress(String ipString)
            throws IllegalArgumentException, UnknownHostException
    {
        if (!Strings.isNullOrEmpty(ipString)) {
            InetAddress address = InetAddresses.forString(ipString);
            if (address.isAnyLocalAddress()) {
                throw new IllegalArgumentException("Wildcard address is not a valid local address: " + address);
            }
            _internalAddress = address;
        } else {
            _internalAddress = InetAddress.getLocalHost();
        }
    }

    public String getInternalAddress()
    {
        return _internalAddress.getHostAddress();
    }

    /**
     * Returns a boolean indicating if the request should be redirected to a
     * pool.
     *
     * @param request a Request
     * @return a boolean indicating if the request should be redirected
     */
    public boolean shouldRedirect(String request)
    {
        switch (request) {
        case "GET":
            return isRedirectOnReadEnabled();
        case "PUT":
            return isRedirectOnWriteEnabled();
        default:
            return false;
        }
    }

    /**
     * Creates a new file. The door will relay all data to the pool.
     * @param path
     * @param inputStream
     * @param length
     * @throws diskCacheV111.util.CacheException
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     */
    public void createFile(FsPath path, InputStream inputStream, Long length)
            throws CacheException, InterruptedException, IOException,
                   URISyntaxException
    {
        Subject subject = getSubject();

        WriteTransfer transfer = new WriteTransfer(_pnfs, subject, path);
        _transfers.put((int) transfer.getSessionId(), transfer);
        try {
            boolean success = false;
            transfer.setProxyTransfer(true);
            transfer.createNameSpaceEntry();
            try {
                transfer.setLength(length);
                try {
                    transfer.selectPoolAndStartMover(_ioQueue, _retryPolicy);
                    String uri = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
                    if (uri == null) {
                        throw new TimeoutCacheException("Server is busy (internal timeout)");
                    }
                    transfer.relayData(inputStream);
                } finally {
                    transfer.killMover(_killTimeout, _killTimeoutUnit);
                }
                success = true;
            } finally {
                if (!success) {
                    transfer.deleteNameSpaceEntry();
                }
            }
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (IOException | RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            _transfers.remove((int) transfer.getSessionId());
        }
    }

    public String getWriteUrl(FsPath path, Long length)
            throws CacheException, InterruptedException,
                   URISyntaxException
    {
        Subject subject = getSubject();

        String uri = null;
        WriteTransfer transfer = new WriteTransfer(_pnfs, subject, path);
        _transfers.put((int) transfer.getSessionId(), transfer);
        try {
            transfer.createNameSpaceEntry();
            try {
                transfer.setLength(length);
                transfer.selectPoolAndStartMover(_ioQueue, _retryPolicy);
                uri = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
                if (uri == null) {
                    throw new TimeoutCacheException("Server is busy (internal timeout)");
                }

                transfer.setStatus("Mover " + transfer.getPool() + "/" +
                        transfer.getMoverId() + ": Waiting for completion");
            } finally {
                if (uri == null) {
                    transfer.deleteNameSpaceEntry();
                }
            }
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                    "Transfer interrupted");
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                    e.toString());
            throw e;
        } finally {
            if (uri == null) {
                _transfers.remove((int) transfer.getSessionId());
            }
        }
        return uri;
    }


    /**
     * Reads the content of a file. The door will relay all data from
     * a pool.
     * @param path
     * @param pnfsid
     * @param outputStream
     * @param range
     * @throws diskCacheV111.util.CacheException
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     */
    public void readFile(FsPath path, PnfsId pnfsid,
                         OutputStream outputStream)
            throws CacheException, InterruptedException, IOException,
                   URISyntaxException
    {
        ReadTransfer transfer = beginRead(path, pnfsid, true);
        try {
            transfer.relayData(outputStream);
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (IOException | RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            _transfers.remove((int) transfer.getSessionId());
        }
    }

    /**
     * Deletes a file.
     * @param pnfsid
     * @param path
     * @throws diskCacheV111.util.CacheException
     */
    public void deleteFile(PnfsId pnfsid, FsPath path)
        throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        pnfs.deletePnfsEntry(pnfsid, path.toString(),
                EnumSet.of(REGULAR, LINK));
        sendRemoveInfoToBilling(path);
    }

    private void sendRemoveInfoToBilling(FsPath path)
    {
        try {
            DoorRequestInfoMessage infoRemove =
                new DoorRequestInfoMessage(getCellAddress().toString(), "remove");
            Subject subject = getSubject();
            infoRemove.setSubject(subject);
            infoRemove.setPath(path);
            infoRemove.setClient(Subjects.getOrigin(subject).getAddress().getHostAddress());
            _billingStub.notify(infoRemove);
        } catch (NoRouteToCellException e) {
            _log.error("Cannot send remove message to billing: {}",
                       e.getMessage());
        }
    }

    /**
     * Deletes a directory.
     * @param pnfsid
     * @param path
     * @throws diskCacheV111.util.CacheException
     */
    public void deleteDirectory(PnfsId pnfsid, FsPath path)
        throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        pnfs.deletePnfsEntry(pnfsid, path.toString(),
                             EnumSet.of(DIR));
    }

    /**
     * Create a new directory.
     * @param parent
     * @param path
     * @throws diskCacheV111.util.CacheException
     */
    public void
        makeDirectory(FileAttributes parent, FsPath path)
        throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        PnfsCreateEntryMessage reply =
            pnfs.createPnfsDirectory(path.toString());
        FileAttributes attributes =
            pnfs.getFileAttributes(reply.getPnfsId(), REQUIRED_ATTRIBUTES);
    }

    public void move(PnfsId pnfsId, FsPath newPath)
        throws CacheException
    {
        PnfsHandler pnfs = new PnfsHandler(_pnfs, getSubject());
        pnfs.renameEntry(pnfsId, newPath.toString());
    }

    /**
     * Returns a read URL for a file.
     *
     * @param path The full path of the file.
     * @param pnfsid The PNFS ID of the file.
     * @return
     * @throws diskCacheV111.util.CacheException
     * @throws java.lang.InterruptedException
     * @throws java.net.URISyntaxException
     */
    public String getReadUrl(FsPath path, PnfsId pnfsid)
            throws CacheException, InterruptedException, URISyntaxException
    {
        return beginRead(path, pnfsid, false).getRedirect();
    }

    /**
     * Initiates a read operation.
     *
     *
     * @param path The full path of the file.
     * @param pnfsid The PNFS ID of the file.
     * @param isProxyTransfer
     * @return ReadTransfer encapsulating the read operation
     */
    private ReadTransfer beginRead(FsPath path, PnfsId pnfsid, boolean isProxyTransfer)
            throws CacheException, InterruptedException, URISyntaxException
    {
        Subject subject = getSubject();

        String uri = null;
        ReadTransfer transfer = new ReadTransfer(_pnfs, subject, path, pnfsid);
        //transfer.setIsChecksumNeeded(isDigestRequested());
        transfer.setIsChecksumNeeded(false);
        _transfers.put((int) transfer.getSessionId(), transfer);
        try {
            transfer.setProxyTransfer(isProxyTransfer);
            transfer.readNameSpaceEntry();
            try {
                transfer.selectPoolAndStartMover(_ioQueue, _retryPolicy);
                uri = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
                if (uri == null) {
                    throw new TimeoutCacheException("Server is busy (internal timeout)");
                }
            } finally {
                transfer.setStatus(null);
            }
            transfer.setStatus("Mover " + transfer.getPool() + "/" +
                               transfer.getMoverId() + ": Waiting for completion");
        } catch (CacheException e) {
            transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (RuntimeException e) {
            transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            if (uri == null) {
                _transfers.remove((int) transfer.getSessionId());
            }
        }
        return transfer;
    }

    /**
     * Message handler for redirect messages from the pools.
     * @param envelope
     * @param message
     */
    public void messageArrived(CellMessage envelope,
                               HttpDoorUrlInfoMessage message)
    {
        HttpTransfer transfer = _transfers.get((int) message.getId());
        if (transfer != null) {
            transfer.redirect(message.getUrl());
        }
    }

    /**
     * Message handler for transfer completion messages from the
     * pools.
     * @param message
     */
    public void messageArrived(DoorTransferFinishedMessage message)
    {
        Transfer transfer = _transfers.get((int) message.getId());
        if (transfer != null) {
            transfer.finished(message);
        }
    }

    /**
     * Returns the current Subject of the calling thread.
     */
    private static Subject getSubject()
    {
        return Subject.getSubject(AccessController.getContext());
    }

    /**
     * Returns the location URI of the current request. This is the
     * full request URI excluding user information, query and fragments.
     */
    private static URI getLocation() throws URISyntaxException
    {
        URI uri = new URI("http://localhost:8543");
        return new URI(uri.getScheme(), null, uri.getHost(),
                uri.getPort(), uri.getPath(), null, null);
    }

    private void initializeTransfer(HttpTransfer transfer, Subject subject)
            throws URISyntaxException
    {
        transfer.setLocation(getLocation());
        transfer.setCellName(getCellName());
        transfer.setDomainName(getCellDomainName());
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPoolStub(_poolStub);
        transfer.setBillingStub(_billingStub);
        transfer.setClientAddress(new InetSocketAddress(Subjects
                .getOrigin(subject).getAddress(),
                PROTOCOL_INFO_UNKNOWN_PORT));
        transfer.setOverwriteAllowed(_isOverwriteAllowed);
    }

    /**
     * Specialisation of the Transfer class for HTTP transfers.
     */
    private class HttpTransfer extends RedirectedTransfer<String>
    {
        private URI _location;
        private InetSocketAddress _clientAddressForPool;

        public HttpTransfer(PnfsHandler pnfs, Subject subject, FsPath path)
                throws URISyntaxException
        {
            super(pnfs, subject, path);
            initializeTransfer(this, subject);
            _clientAddressForPool = getClientAddress();
        }

        protected ProtocolInfo createProtocolInfo(InetSocketAddress address)
        {
            HttpProtocolInfo protocolInfo =
                new HttpProtocolInfo(
                        PROTOCOL_INFO_NAME,
                        PROTOCOL_INFO_MAJOR_VERSION,
                        PROTOCOL_INFO_MINOR_VERSION,
                        address,
                        getCellName(), getCellDomainName(),
                        _path.toString(),
                        _location);
            protocolInfo.setSessionId((int) getSessionId());
            return protocolInfo;
        }

        @Override
        protected ProtocolInfo getProtocolInfoForPoolManager()
        {
            return createProtocolInfo(getClientAddress());
        }

        @Override
        protected ProtocolInfo getProtocolInfoForPool()
        {
            return createProtocolInfo(_clientAddressForPool);
        }

        public void setLocation(URI location)
        {
            _location = location;
        }

        public void setProxyTransfer(boolean isProxyTransfer)
        {
            if (isProxyTransfer) {
                _clientAddressForPool = new InetSocketAddress(_internalAddress, 0);
            } else {
                _clientAddressForPool = getClientAddress();
            }
        }
    }

    /**
     * Specialised HttpTransfer for downloads.
     */
    private class ReadTransfer extends HttpTransfer
    {
        public ReadTransfer(PnfsHandler pnfs, Subject subject,
                            FsPath path, PnfsId pnfsid)
                throws URISyntaxException
        {
            super(pnfs, subject, path);
            setPnfsId(pnfsid);
        }

        public void setIsChecksumNeeded(boolean isChecksumNeeded)
        {
            if(isChecksumNeeded) {
                setAdditionalAttributes(Collections.singleton(CHECKSUM));
            } else {
                setAdditionalAttributes(Collections.<FileAttribute>emptySet());
            }
        }

        public void relayData(OutputStream outputStream)
            throws IOException, CacheException, InterruptedException
        {
            setStatus("Mover " + getPool() + "/" + getMoverId() +
                      ": Opening data connection");
            try {
                URL url = new URL(getRedirect());
                HttpURLConnection connection =
                    (HttpURLConnection) url.openConnection();
                try {
                    connection.setRequestProperty("Connection", "Close");
                    connection.connect();
                    try (InputStream inputStream = connection
                            .getInputStream()) {
                        setStatus("Mover " + getPool() + "/" + getMoverId() +
                                ": Sending data");
                        ByteStreams.copy(inputStream, outputStream);
                        outputStream.flush();
                    }

                } finally {
                    connection.disconnect();
                }

                if (!waitForMover(_transferConfirmationTimeout, _transferConfirmationTimeoutUnit)) {
                    throw new CacheException("Missing transfer confirmation from pool");
                }
            } catch (SocketTimeoutException e) {
                throw new TimeoutCacheException("Server is busy (internal timeout)");
            } finally {
                setStatus(null);
            }
        }

        @Override
        public synchronized void finished(CacheException error)
        {
            super.finished(error);

            _transfers.remove((int) getSessionId());

            if (error == null) {
                notifyBilling(0, "");
            } else {
                notifyBilling(error.getRc(), error.getMessage());
            }
        }
    }

    /**
     * Specialised HttpTransfer for uploads.
     */
    private class WriteTransfer extends HttpTransfer
    {
        public WriteTransfer(PnfsHandler pnfs, Subject subject, FsPath path)
                throws URISyntaxException
        {
            super(pnfs, subject, path);
        }

        public void relayData(InputStream inputStream)
                throws IOException, CacheException, InterruptedException
        {
            setStatus("Mover " + getPool() + "/" + getMoverId() +
                    ": Opening data connection");
            try {
                URL url = new URL(getRedirect());
                HttpURLConnection connection =
                        (HttpURLConnection) url.openConnection();
                try {
                    connection.setRequestMethod("PUT");
                    connection.setRequestProperty("Connection", "Close");
                    connection.setDoOutput(true);
                    if (getFileAttributes().isDefined(SIZE)) {
                        connection.setFixedLengthStreamingMode(getFileAttributes().getSize());
                    } else {
                        connection.setChunkedStreamingMode(8192);
                    }
                    connection.connect();
                    try (OutputStream outputStream = connection.getOutputStream()) {
                        setStatus("Mover " + getPool() + "/" + getMoverId() +
                                ": Receiving data");
                        ByteStreams.copy(inputStream, outputStream);
                        outputStream.flush();
                    }
                    if (connection.getResponseCode() != HttpResponseStatus.CREATED.getCode()) {
                        throw new CacheException(connection.getResponseMessage());
                    }
                } finally {
                    connection.disconnect();
                }

                if (!waitForMover(_transferConfirmationTimeout, _transferConfirmationTimeoutUnit)) {
                    throw new CacheException("Missing transfer confirmation from pool");
                }
            } catch (SocketTimeoutException e) {
                throw new TimeoutCacheException("Server is busy (internal timeout)");
            } finally {
                setStatus(null);
            }
        }

        /**
         * Sets the length of the file to be uploaded. The length is
         * optional and will be ignored if null.
         */
        public void setLength(Long length)
        {
            if (length != null) {
                super.setLength(length);
            }
        }

        @Override
        public synchronized void finished(CacheException error)
        {
            super.finished(error);

            _transfers.remove((int) getSessionId());

            if (error == null) {
                notifyBilling(0, "");
            } else {
                notifyBilling(error.getRc(), error.getMessage());
            }
        }
    }
}
