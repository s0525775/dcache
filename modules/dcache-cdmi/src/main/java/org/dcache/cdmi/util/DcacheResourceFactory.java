package org.dcache.cdmi.util;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.net.InetAddresses;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
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
import java.util.List;
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
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.auth.Subjects;
import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;
import org.dcache.util.TransferRetryPolicy;

import dmg.cells.nucleus.CellAddressCore;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.dcache.namespace.FileAttribute.*;

/**
 * This ResourceFactory exposes the dCache name space through the
 * Milton WebDAV framework.
 */
public class DcacheResourceFactory
    implements CellMessageReceiver, CellCommandListener
{
    private static final Logger _log =
        LoggerFactory.getLogger(DcacheResourceFactory.class);

    private static final String PROTOCOL_INFO_NAME = "Http";
    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;

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

    private int _moverTimeout = 180000;
    private TimeUnit _moverTimeoutUnit = MILLISECONDS;
    private long _killTimeout = 1500;
    private TimeUnit _killTimeoutUnit = MILLISECONDS;
    private long _transferConfirmationTimeout = 60000;
    private TimeUnit _transferConfirmationTimeoutUnit = MILLISECONDS;
    private CellStub _poolStub;
    private CellStub _poolManagerStub;
    private CellStub _billingStub;
    private PnfsHandler _pnfs;
    private String _cellName;
    private String _cellDomainName;
    private CellAddressCore _cellAddress;
    private String _ioQueue;
    private InetAddress _internalAddress;
    private boolean _isOverwriteAllowed;

    private TransferRetryPolicy _retryPolicy =
        TransferRetryPolicies.tryOncePolicy(_moverTimeout, _moverTimeoutUnit);

    public DcacheResourceFactory(String cellName, String cellDomainName, CellAddressCore cellAddress,
                                 PnfsHandler pnfsHandler, CellStub poolStub, CellStub poolManagerStub,
                                 CellStub billingStub) throws UnknownHostException
    {
        _cellName = cellName;
        _cellDomainName = cellDomainName;
        _cellAddress = cellAddress;
        _pnfs = pnfsHandler;
        _poolStub = poolStub;
        _poolManagerStub = poolManagerStub;
        _billingStub = billingStub;

        _internalAddress = InetAddress.getLocalHost();
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
     * Creates a new file. The door will relay all data to the pool.
     */
    public void createFile(FsPath path, InputStream inputStream, Long length)
            throws CacheException, InterruptedException, IOException,
                   URISyntaxException
    {
        Subject subject = getSubject();
        subject = Subjects.ROOT; //temp

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
                    //String uri = transfer.waitForRedirect(_moverTimeout, _moverTimeoutUnit);
                    //_log.warn("URI001:", uri);
                    //if (uri == null) {
                    //    throw new TimeoutCacheException("Server is busy (internal timeout)");
                    //}
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
            _log.warn("DCRF:" + e.getMessage());
            //transfer.notifyBilling(e.getRc(), e.getMessage());
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

    /*
    public String getWriteUrl(FsPath path, Long length)
            throws CacheException, InterruptedException,
                   URISyntaxException
    {
        Subject subject = getSubject();
        subject = Subjects.ROOT; //temp

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
    */

    /**
     * Reads the content of a file. The door will relay all data from
     * a pool.
     */
    public void readFile(FsPath path, PnfsId pnfsid,
                         OutputStream outputStream, int start, int end)
            throws CacheException, InterruptedException, IOException,
                   URISyntaxException
    {
        ReadTransfer transfer = beginRead(path, pnfsid, true);
        try {
            transfer.relayData(outputStream, start, end);
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

    private void sendRemoveInfoToBilling(FsPath path)
    {
        try {
            DoorRequestInfoMessage infoRemove =
                new DoorRequestInfoMessage(_cellAddress.toString(), "remove");
            Subject subject = getSubject();
            infoRemove.setSubject(subject);
            infoRemove.setPath(path.toString());
            infoRemove.setClient(Subjects.getOrigin(subject).getAddress().getHostAddress());
            _billingStub.send(infoRemove);
        } catch (NoRouteToCellException e) {
            _log.error("Cannot send remove message to billing: {}",
                       e.getMessage());
        }
    }

    /**
     * Returns a read URL for a file.
     *
     * @param path The full path of the file.
     * @param pnfsid The PNFS ID of the file.
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
        subject = Subjects.ROOT; //temp

        String uri = null;
        ReadTransfer transfer = new ReadTransfer(_pnfs, subject, path, pnfsid);
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

    private void initializeTransfer(HttpTransfer transfer, Subject subject)
            throws URISyntaxException
    {
        transfer.setLocation(URI.create("/disk/test2.txt"));
        transfer.setCellName(_cellName);
        transfer.setDomainName(_cellDomainName);
        transfer.setPoolManagerStub(_poolManagerStub);
        transfer.setPoolStub(_poolStub);
        transfer.setBillingStub(_billingStub);
        //transfer.setClientAddress(new InetSocketAddress(Subjects
        //        .getOrigin(subject).getAddress(),
        //        PROTOCOL_INFO_UNKNOWN_PORT));
        transfer.setClientAddress(new InetSocketAddress(_internalAddress, 0));
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
            _log.warn("GG005");
        }

        protected ProtocolInfo createProtocolInfo(InetSocketAddress address)
        {
            _log.warn("GG006");
            HttpProtocolInfo protocolInfo =
                new HttpProtocolInfo(
                        PROTOCOL_INFO_NAME,
                        PROTOCOL_INFO_MAJOR_VERSION,
                        PROTOCOL_INFO_MINOR_VERSION,
                        address,
                        _cellName, _cellDomainName,
                        _path.toString(),
                        _location);
            protocolInfo.setSessionId((int) getSessionId());
            return protocolInfo;
        }

        @Override
        protected ProtocolInfo getProtocolInfoForPoolManager()
        {
            _log.warn("GG007");
            return createProtocolInfo(getClientAddress());
        }

        @Override
        protected ProtocolInfo getProtocolInfoForPool()
        {
            _log.warn("GG008");
            return createProtocolInfo(_clientAddressForPool);
        }

        public void setLocation(URI location)
        {
            _log.warn("GG009");
            _location = location;
        }

        public void setProxyTransfer(boolean isProxyTransfer)
        {
            _log.warn("GG010");
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

        public void relayData(OutputStream outputStream, int start, int end)
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
                    if (start < 0 && end <= start) {
                        connection.addRequestProperty("Range", String.format("bytes=%d-%d", start, end));
                    }

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
                _log.warn("GG004:" + getRedirect());
                URL url = new URL(getRedirect());
                _log.warn("GG004_0");
                HttpURLConnection connection =
                        (HttpURLConnection) url.openConnection();  //is null
                _log.warn("GG004_00");
                try {
                    _log.warn("GG004_1");
                    connection.setRequestMethod("PUT");
                    _log.warn("GG004_2");
                    connection.setRequestProperty("Connection", "Close");
                    _log.warn("GG004_3");
                    connection.setDoOutput(true);
                    _log.warn("GG004_4");
                    if (getFileAttributes().isDefined(SIZE)) {
                        _log.warn("GG004_5:" + getFileAttributes().getSize());
                        //connection.setFixedLengthStreamingMode(getFileAttributes().getSize());
                    } else {
                        _log.warn("GG004_6");
                        connection.setChunkedStreamingMode(8192);
                    }
                    _log.warn("GG004_7");
                    connection.connect();
                    try (OutputStream outputStream = connection.getOutputStream()) {
                        setStatus("Mover " + getPool() + "/" + getMoverId() +
                                ": Receiving data");
                        ByteStreams.copy(inputStream, outputStream);
                        outputStream.flush();
                    }
                    if (connection.getResponseCode() != HttpResponseStatus.CREATED.getCode()) {
                        _log.warn("GG004_8:" + connection.getResponseMessage());
                        throw new CacheException(connection.getResponseMessage());
                    }
                } finally {
                    _log.warn("GG004_9");
                    connection.disconnect();
                }

                _log.warn("GG004_10");
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
