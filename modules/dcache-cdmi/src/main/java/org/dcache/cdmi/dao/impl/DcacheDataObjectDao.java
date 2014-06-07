/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.cdmi.dao.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.collect.Maps;
import static org.dcache.namespace.FileAttribute.*;
import com.google.common.collect.Range;
import com.google.common.io.ByteStreams;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorTransferFinishedMessage;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.ProtocolInfo;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.cdmi.mover.CDMIProtocolInfo;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.cdmi.model.DcacheDataObject;
import org.dcache.cdmi.util.IdConverter;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.PingMoversTask;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.dao.DataObjectDao;
import org.snia.cdmiserver.exception.BadRequestException;
import org.snia.cdmiserver.exception.ConflictException;
import org.snia.cdmiserver.model.DataObject;

/* This class is dCache's DAO implementation class for SNIA's DataObjectDao interface.
   DataObject represents a file in the CDMI standard.
   This class contains all operations which are related to file operations, such as get/create/update a file.
   Updating a file is still not supported here, but this support will be added here very soon.
   Getting/Creating/Updating/Deleting a file via ObjectID still isn't supported, the support shall still get added here.
*/

/**
 * <p>
 * Concrete implementation of {@link DataObjectDao} using the local filesystem as the backing store.
 * </p>
 */
public class DcacheDataObjectDao extends AbstractCellComponent
    implements DataObjectDao, CellLifeCycleAware, CellMessageReceiver
{

    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcacheDataObjectDao.class);

    //
    // Properties and Dependency Injection Methods by CDMI
    //
    private String baseDirectoryName = null;

    //
    // Properties and Dependency Injection Methods by dCache
    //
    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES =
        EnumSet.of(PNFSID, CREATION_TIME, ACCESS_TIME, CHANGE_TIME, MODIFICATION_TIME, TYPE, SIZE, MODE, ACL, OWNER, OWNER_GROUP);
    private static final String PROTOCOL_INFO_NAME = "Http";
    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;
    private static final int PROTOCOL_INFO_UNKNOWN_PORT = 0;
    private static final long PING_DELAY = 300000;
    private static final int NOTFOUND_ERROR = 1;
    private static final int PERMISSION_ERROR = 2;
    private static final int PROVIDER_ERROR = 3;
    private final Map<Integer,HttpTransfer> _transfers = Maps.newConcurrentMap();
    private ScheduledExecutorService _executor;
    private CellStub pnfsStub;
    private PnfsHandler pnfsHandler;
    private ListDirectoryHandler listDirectoryHandler;
    private CellStub poolStub;
    private CellStub poolMgrStub;
    private CellStub billingStub;
    private PnfsId pnfsId;
    private FileAttributes _attributes;
    private SocketChannel socketChannel = null;
    private ObjectOutputStream  oos = null;
    private ObjectInputStream ois = null;
    private String host = "localhost";
    private static final int port = 9999;
    private static final int maxTries = 5;
    private int _moverTimeout = 180000;
    private TimeUnit _moverTimeoutUnit = MILLISECONDS;
    private long _killTimeout = 1500;
    private TimeUnit _killTimeoutUnit = MILLISECONDS;
    private long _transferConfirmationTimeout = 60000;
    private TimeUnit _transferConfirmationTimeoutUnit = MILLISECONDS;
    private int _bufferSize = 65536;
    private String _ioQueue;
    private InetAddress _internalAddress;
    private boolean _doRedirectOnRead = true;
    private boolean _doRedirectOnWrite = true;
    private boolean _isOverwriteAllowed;
    private boolean _isAnonymousListingAllowed;
    private TransferRetryPolicy _retryPolicy =
        TransferRetryPolicies.tryOncePolicy(_moverTimeout, _moverTimeoutUnit);

    /**
     * <p>
     * Set the base directory name for our local storage.
     * </p>
     *
     * @param baseDirectoryName
     */
    public void setBaseDirectoryName(String baseDirectoryName)
    {
        this.baseDirectoryName = baseDirectoryName;
        _log.trace("BaseDirectory(O)={}", baseDirectoryName);
    }

    /**
     * <p>
     * Set the PnfsStub from dCache.
     * </p>
     *
     * @param pnfsStub
     */
    public void setPnfsStub(CellStub pnfsStub)
    {
        this.pnfsStub = checkNotNull(pnfsStub);
        this.pnfsHandler = new PnfsHandler(this.pnfsStub);
    }

    /**
     * <p>
     * Set the ListDirectoryHandler from dCache.
     * </p>
     *
     * @param listDirectoryHandler
     */
    public void setListDirectoryHandler(ListDirectoryHandler listDirectoryHandler)
    {
        this.listDirectoryHandler = checkNotNull(listDirectoryHandler);
    }

    /**
     * <p>
     * Set the PoolStub from dCache.
     * </p>
     *
     * @param poolStub
     */
    public void setPoolStub(CellStub poolStub)
    {
        this.poolStub = checkNotNull(poolStub);
    }

    /**
     * <p>
     * Set the PoolMgrStub from dCache.
     * </p>
     *
     * @param poolMgrStub
     */
    public void setPoolMgrStub(CellStub poolMgrStub)
    {
        this.poolMgrStub = checkNotNull(poolMgrStub);
    }

    /**
     * <p>
     * Set the BillingStub from dCache.
     * </p>
     *
     * @param billingStub
     */
    public void setBillingStub(CellStub billingStub)
    {
        this.billingStub = checkNotNull(billingStub);
    }

    public void setAnonymousListing(boolean isAllowed)
    {
        _isAnonymousListingAllowed = isAllowed;
    }

    public boolean isAnonymousListing()
    {
        return _isAnonymousListingAllowed;
    }

    public String getInternalAddress()
    {
        return _internalAddress.getHostAddress();
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

    public DcacheDataObjectDao() throws UnknownHostException
    {
        _internalAddress = InetAddress.getLocalHost();
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
            return _doRedirectOnRead;
        case "PUT":
            return _doRedirectOnWrite;
        default:
            return false;
        }
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


    /**
     * <p>
     * Gets the ContainerName for a requested path.
     * </p>
     *
     * @param path
     */
    String getcontainerName(String path)
    {
        // Make sure we have a file name for the object
        // check for file name
        // path should be <container name>/<file name>
        // Split path into path and filename
        String[] tokens = path.split("/");
        if (tokens.length < 1) {
            throw new BadRequestException("No object name in path <" + path + ">");
        }
        String containerName = "";
        for (int i = 0; i <= tokens.length - 2; i++) {
            containerName += tokens[i] + "/";
        }
        return containerName;
    }

    //
    // DataObjectDao Methods invoked from PathResource
    //
    @Override
    public DcacheDataObject createByPath(String path, DataObject dObj) throws Exception {
        //
        String containerName = getcontainerName(path);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        DcacheDataObject newDObj = (DcacheDataObject) dObj;

        //
        File objFile, baseDirectory, containerDirectory;
        try {
            _log.trace("baseDirectory={}", baseDirectoryName);
            baseDirectory = new File(baseDirectoryName + "/");
            _log.trace("Base Directory AbsolutePath={}", baseDirectory.getAbsolutePath());
            containerDirectory = new File(baseDirectory, containerName);
            _log.trace("Container AbsolutePath={}", containerDirectory.getAbsolutePath());
            //
            objFile = new File(baseDirectory, path);
            _log.trace("Object AbsolutePath={}", objFile.getAbsolutePath());
        } catch (Exception ex) {
            _log.error("Exception while writing, {}", ex);
            throw new IllegalArgumentException("Cannot write Object @" + path + ", error: " + ex);
        }

        // check for container
        if (!checkIfDirectoryFileExists(containerDirectory.getAbsolutePath())) {
            throw new ConflictException("Container <"
                                        + containerDirectory.getAbsolutePath()
                                        + "> doesn't exist");
        }
        if (checkIfDirectoryFileExists(objFile.getAbsolutePath())) {
            throw new ConflictException("Object File <" + objFile.getAbsolutePath() + "> exists");
        }
        try {
            // Make object ID
            newDObj.setObjectType("application/cdmi-object");
            newDObj.setCapabilitiesURI("/cdmi_capabilities/dataobject");
            FsPath fsPath = new FsPath(objFile.getAbsolutePath());
            byte[] bytData = newDObj.getValue().getBytes(StandardCharsets.UTF_8);
            InputStream isData = new ByteArrayInputStream(bytData);
            if (!writeFile(fsPath, isData, (long) bytData.length)) {
                _log.error("Exception while writing.");
                throw new IllegalArgumentException("Cannot write Object file @"
                                                   + path);
            }

            //update ObjectID with correct ObjectID
            String objectID = "";
            int oowner = 0;
            ACL oacl = null;
            FileAttributes attr = getAttributesByPath(objFile.getAbsolutePath());
            if (attr != null) {
                pnfsId = attr.getPnfsId();
                if (pnfsId != null) {
                    // update with real info
                    _log.trace("DCacheDataObjectDao<Create>, setPnfsID={}", pnfsId.toIdString());
                    newDObj.setPnfsID(pnfsId.toIdString());
                    long ctime = attr.getCreationTime();
                    long atime = attr.getAccessTime();
                    long mtime = attr.getModificationTime();
                    long osize = attr.getSize();
                    newDObj.setMetadata("cdmi_ctime", sdf.format(ctime));
                    newDObj.setMetadata("cdmi_atime", sdf.format(atime));
                    newDObj.setMetadata("cdmi_mtime", sdf.format(mtime));
                    newDObj.setMetadata("cdmi_size", String.valueOf(osize));
                    oowner = attr.getOwner();
                    oacl = attr.getAcl();
                    objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                    newDObj.setObjectID(objectID);
                    _log.trace("DCacheDataObjectDao<Create>, setObjectID={}", objectID);
                } else {
                    _log.error("DCacheDataObjectDao<Create>, Cannot read PnfsId from meta information, ObjectID will be empty");
                }
            } else {
                _log.error("DCacheDataObjectDao<Create>, Cannot read meta information from directory {}", objFile.getAbsolutePath());
            }

            // Add metadata
            newDObj.setMetadata("cdmi_acount", "0");
            newDObj.setMetadata("cdmi_mcount", "0");
            newDObj.setMetadata("cdmi_owner", String.valueOf(oowner));  //TODO: need to implement authentification and authorization first
            if (oacl != null && !oacl.isEmpty()) {
                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                for (ACE ace : oacl.getList()) {
                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                    subMetadataEntry_ACL.put("acetype", ace.getType().name());
                    subMetadataEntry_ACL.put("identifier", ace.getWho().name());
                    subMetadataEntry_ACL.put("aceflags", String.valueOf(ace.getFlags()));
                    subMetadataEntry_ACL.put("acemask", String.valueOf(ace.getAccessMsk()));
                    subMetadata_ACL.add(subMetadataEntry_ACL);
                }
                newDObj.setSubMetadata_ACL(subMetadata_ACL);
            } else {
                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                subMetadataEntry_ACL.put("acetype", "ALLOW");
                subMetadataEntry_ACL.put("identifier", "OWNER@");
                subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                subMetadata_ACL.add(subMetadataEntry_ACL);
                newDObj.setSubMetadata_ACL(subMetadata_ACL);
            }

            String mimeType = newDObj.getMimetype();
            if (mimeType == null) {
                mimeType = "text/plain";
            }
            newDObj.setMimetype(mimeType);
            newDObj.setMetadata("mimetype", mimeType);
            newDObj.setMetadata("fileName", objFile.getAbsolutePath());
            //
        } catch (IllegalArgumentException ex) {
            _log.error("Exception while writing={}", ex);
            throw new IllegalArgumentException("Cannot write Object @" + path + ", error: " + ex);
        }
        return newDObj;
    }

    @Override
    public void deleteByPath(String path)
    {
        throw new UnsupportedOperationException("DcacheDataObjectDao.deleteByPath()");
    }

    @Override
    public DcacheDataObject findByPath(String path)
    {

        // ISO-8601 Date
        Date now = new Date();
        long nowAsLong = now.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        _log.trace("In DCacheDataObjectDao.findByPath, Path={}", path);

        String containerName = getcontainerName(path);

        // Check for object file
        File objFile, baseDirectory;
        try {
            _log.trace("baseDirectory={}", baseDirectoryName);
            baseDirectory = new File(baseDirectoryName + "/");
            objFile = new File(baseDirectory, path);
            _log.trace("Object AbsolutePath={}", objFile.getAbsolutePath());
        } catch (Exception ex) {
            _log.error("Exception in findByPath={}", ex);
            throw new IllegalArgumentException("Cannot get Object @" + path + ", error: " + ex);
        }

        if (!checkIfDirectoryFileExists(objFile.getAbsolutePath())) {
            return null;
        }
        //
        // Both Files are there. So open, read, create object and send out
        //
        DcacheDataObject dObj = new DcacheDataObject();
        try {
            // Read object from file
            FsPath fsPath = new FsPath(objFile.getAbsolutePath());
            if (getResource(fsPath) == 0) {
                if (_attributes.getFileType() != FileType.DIR) {
                    pnfsId = _attributes.getPnfsId();
                    byte[] inBytes = readFile(fsPath, pnfsId);
                    if (inBytes != null) {
                        dObj.setValue(new String(inBytes));
                    }
                }
            }

            dObj.setObjectType("application/cdmi-object");
            dObj.setCapabilitiesURI("/cdmi_capabilities/dataobject");

            //update ObjectID with correct ObjectID
            String objectID = "";
            int oowner = 0;
            ACL oacl = null;
            FileAttributes attr = getAttributesByPath(objFile.getAbsolutePath());
            if (attr != null) {
                pnfsId = attr.getPnfsId();
                if (pnfsId != null) {
                    // update with real info
                    _log.trace("DCacheDataObjectDao<Read>, setPnfsID={}", pnfsId.toIdString());
                    dObj.setPnfsID(pnfsId.toIdString());
                    long ctime = attr.getCreationTime();
                    long atime = attr.getAccessTime();
                    long mtime = attr.getModificationTime();
                    long osize = attr.getSize();
                    dObj.setMetadata("cdmi_ctime", sdf.format(ctime));
                    dObj.setMetadata("cdmi_atime", sdf.format(atime));
                    dObj.setMetadata("cdmi_mtime", sdf.format(mtime));
                    dObj.setMetadata("cdmi_size", String.valueOf(osize));
                    oowner = attr.getOwner();
                    oacl = attr.getAcl();
                    objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                    dObj.setObjectID(objectID);
                    _log.trace("DCacheDataObjectDao<Read>, setObjectID={}", objectID);
                } else {
                    _log.error("DCacheDataObjectDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
                }
            } else {
                _log.error("DCacheDataObjectDao<Read>, Cannot read meta information from object {}", objFile.getAbsolutePath());
            }

            dObj.setMetadata("cdmi_acount", "0");
            dObj.setMetadata("cdmi_mcount", "0");
            dObj.setMetadata("cdmi_owner", String.valueOf(oowner));  //TODO: need to implement authentication and autorization first
            if (oacl != null && !oacl.isEmpty()) {
                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                for (ACE ace : oacl.getList()) {
                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                    subMetadataEntry_ACL.put("acetype", ace.getType().name());
                    subMetadataEntry_ACL.put("identifier", ace.getWho().name());
                    subMetadataEntry_ACL.put("aceflags", String.valueOf(ace.getFlags()));
                    subMetadataEntry_ACL.put("acemask", String.valueOf(ace.getAccessMsk()));
                    subMetadata_ACL.add(subMetadataEntry_ACL);
                }
                dObj.setSubMetadata_ACL(subMetadata_ACL);
            } else {
                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                subMetadataEntry_ACL.put("acetype", "ALLOW");
                subMetadataEntry_ACL.put("identifier", "OWNER@");
                subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                subMetadata_ACL.add(subMetadataEntry_ACL);
                dObj.setSubMetadata_ACL(subMetadata_ACL);
            }

            String mimeType = dObj.getMimetype();
            if (mimeType == null) {
                mimeType = "text/plain";
            }
            dObj.setMimetype(mimeType);
            dObj.setMetadata("mimetype", mimeType);
            dObj.setMetadata("fileName", objFile.getAbsolutePath());

        } catch (Exception ex) {
            _log.error("Exception while reading, {}", ex);
            throw new IllegalArgumentException("Cannot read Object @" + path + " error: " + ex);
        }

        // change access time
        try {
            FileAttributes attr = new FileAttributes();
            attr.setAccessTime(nowAsLong);
            pnfsHandler.setFileAttributes(pnfsId, attr);
            dObj.setMetadata("cdmi_atime", sdf.format(now));
        } catch (CacheException ex) {
            _log.error("DCacheDataObjectDao<Read>, Cannot update meta information for object with objectID {}", dObj.getObjectID());
        }

        return dObj;
    }

    @Override
    public DcacheDataObject findByObjectId(String objectId)
    {
        throw new UnsupportedOperationException("DcacheDataObjectDao.findByObjectId()");
    }

    /**
     * DCache related stuff.
     */
    @Override
    public void afterStart()
    {
        _log.trace("Start DCacheDataObjectDao");
    }

    @Override
    public void beforeStop()
    {
    }

    /**
     * <p>
     * Gets the parent directory of a file path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private String getParentDirectory(String path)
    {
        String result = "/";
        if (path != null) {
            String tempPath = path;
            if (path.endsWith("/")) {
                tempPath = path.substring(0, path.length() - 1);
            }
            String parent = tempPath;
            if (tempPath.contains("/")) {
                parent = tempPath.substring(0, tempPath.lastIndexOf("/"));
            }
            if (parent != null) {
                if (parent.isEmpty()) {
                    result = "/";
                } else {
                    result = parent;
                }
            }
        }
        return result;
    }

    /**
     * <p>
     * Gets the last item (file or directory) of a file path, opposite of getParentDirectory.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private String getItem(String path)
    {
        String result = "";
        String tempPath = path;
        if (path != null) {
            if (path.endsWith("/")) {
                tempPath = path.substring(0, path.length() - 1);
            }
            String item = tempPath;
            if (tempPath.contains("/")) {
                item = tempPath.substring(tempPath.lastIndexOf("/") + 1, tempPath.length());
            }
            if (item != null) {
                result = item;
            }
        }
        return result;
    }

    /**
     * <p>
     * Checks if a Directory or File exists in a specific path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean checkIfDirectoryFileExists(String dirPath)
    {
        boolean result = false;
        String searchedItem = getItem(dirPath);
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        Map<String, FileAttributes> listing = listDirectoriesFilesByPath(getParentDirectory(tmpDirPath));
        for (Map.Entry<String, FileAttributes> entry : listing.entrySet()) {
            if (entry.getKey().compareTo(searchedItem) == 0) {
                result = true;
            }
        }
        return result;
    }

    /**
     * <p>
     * Lists all File Attributes for specific directory.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private FileAttributes getAttributesByPath(String path)
    {
        FileAttributes result = null;
        String searchedItem = getItem(path);
        String tmpDirPath = addPrefixSlashToPath(path);
        Map<String, FileAttributes> listing = listDirectoriesFilesByPath(getParentDirectory(tmpDirPath));
        for (Map.Entry<String, FileAttributes> entry : listing.entrySet()) {
            if (entry.getKey().compareTo(searchedItem) == 0) {
                result = entry.getValue();
            }
        }
        return result;
    }

    /**
     * <p>
     * Lists all Directories and Files in a specific path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private Map<String, FileAttributes> listDirectoriesFilesByPath(String path)
    {
        String tmpPath = addPrefixSlashToPath(path);
        FsPath fsPath = new FsPath(tmpPath);
        Map<String, FileAttributes> result = new HashMap<>();
        try {
            listDirectoryHandler.printDirectory(Subjects.ROOT, new ListPrinter(result), fsPath, null, Range.<Integer>all());
        } catch (InterruptedException | CacheException ex) {
            _log.warn("DcacheDataObjectDao, Directory and file listing for path '{}' was not possible, internal error message={}", path, ex.getMessage());
        }
        return result;
    }

    /**
     * <p>
     * Adds a prefix slash to a directory path if there isn't a slash already.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private String addPrefixSlashToPath(String path)
    {
        String result = "";
        if (path != null && path.length() > 0) {
            if (!path.startsWith("/")) {
                result = "/" + path;
            } else {
                result = path;
            }
        }
        return result;
    }

    @Override
    public DcacheDataObject createById(String string, DataObject d)
    {
        throw new UnsupportedOperationException("DcacheDataObjectDao, Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    /**
     * <p>
     * Lists all object in a parent directory including FileAttributes.
     * </p>
     */
    private static class ListPrinter implements DirectoryListPrinter
    {
        private final Map<String, FileAttributes> list;

        private ListPrinter(Map<String, FileAttributes> list)
        {
            this.list = list;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.of(PNFSID, CREATION_TIME, ACCESS_TIME, CHANGE_TIME, MODIFICATION_TIME, TYPE, SIZE, ACL, OWNER);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
                throws InterruptedException
        {
            FileAttributes attr = entry.getFileAttributes();
            list.put(entry.getName(), attr);
        }
    }

    /**
     * <p>
     * Writes a File to dCache and fetches FileAttributes, simple way.
     * </p>
     * @param filePath
     * @param data
     * @return
     */
    public boolean writeFile2(String filePath, String data)
    {
        boolean result = false;
        try {
            //The order of all commands is very important!
            Subject subject = Subjects.ROOT;
            CDMIProtocolInfo cdmiProtocolInfo = new CDMIProtocolInfo(new InetSocketAddress(InetAddress.getLocalHost(), 0));
            Transfer transfer = new Transfer(pnfsHandler, subject, new FsPath(filePath));
            transfer.setClientAddress(new InetSocketAddress(InetAddress.getLocalHost(), 0));
            transfer.setPoolStub(poolStub);
            transfer.setPoolManagerStub(poolMgrStub);
            transfer.setBillingStub(billingStub);
            transfer.setCellName(getCellName());
            transfer.setDomainName(getCellDomainName());
            transfer.setProtocolInfo(cdmiProtocolInfo);
            transfer.setOverwriteAllowed(true);
            try {
                transfer.createNameSpaceEntryWithParents();
                try {
                    transfer.selectPoolAndStartMover(null, TransferRetryPolicies.tryOncePolicy(5000));
                    write(data);
                }
                finally {
                    pnfsId = transfer.getPnfsId();
                }
                if (pnfsId != null) _log.trace("DcacheDataObjectDao<Write>-pnfsId={}", pnfsId);
                _log.trace("DcacheDataObjectDao<Write>-data={}", data);
                result = true;
            } finally {
            }
        } catch (CacheException | InterruptedException | UnknownHostException ex) {
            _log.error("DcacheDataObjectDao, File could not become written, {}", ex.getMessage());
        }
        return result;
    }

    /**
     * <p>
     * Reads a File from dCache and fetches FileAttributes, simple way.
     * </p>
     * @param filePath
     * @return
     */
    public String readFile2(String filePath)
    {
        String result = "";
        try {
            //The order of all commands is very important!
            Subject subject = Subjects.ROOT;
            CDMIProtocolInfo cdmiProtocolInfo = new CDMIProtocolInfo(new InetSocketAddress(InetAddress.getLocalHost(), 0));
            Transfer transfer = new Transfer(pnfsHandler, subject, new FsPath(filePath));
            transfer.setClientAddress(new InetSocketAddress(InetAddress.getLocalHost(), 0));
            transfer.setPoolStub(poolStub);
            transfer.setPoolManagerStub(poolMgrStub);
            transfer.setBillingStub(billingStub);
            transfer.setCellName(getCellName());
            transfer.setDomainName(getCellDomainName());
            transfer.setProtocolInfo(cdmiProtocolInfo);
            try {
                transfer.readNameSpaceEntry();
                try {
                    transfer.selectPoolAndStartMover(null, TransferRetryPolicies.tryOncePolicy(5000));
                    result = read();
                    _log.trace("DcacheDataObjectDao received data={}", result.toString());
                }
                finally {
                    pnfsId = transfer.getFileAttributes().getPnfsId();
                }
                _log.trace("DcacheDataObjectDao<Read>-isWrite={}", transfer.isWrite());
                if (pnfsId != null) _log.trace("DcacheDataObjectDao<Read>-pnfsId={}", pnfsId);
                if (result != null) _log.trace("DcacheDataObjectDao<Read>-data={}", result.toString());
            } finally {
            }
        } catch (CacheException | InterruptedException | UnknownHostException ex) {
            _log.error("DcacheDataObjectDao, File could not become read, {}", ex.getMessage());
        }
        return result;
    }

    public synchronized void connect()
    {
        int tries = 0;
        boolean connected = false;
        try {
            host = InetAddress.getLocalHost().getHostName();
            while ((!connected) && (tries < maxTries)) {
                try {
                    socketChannel = SocketChannel.open();
                    connected = socketChannel.connect(new InetSocketAddress(host, port));
                    tries = tries + 1;
                } catch (ConnectException ex) {
                }
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    private synchronized void write(String data)
    {
        connect();
        try {
            if ((socketChannel != null) && socketChannel.isConnected()) {
                oos = new ObjectOutputStream(socketChannel.socket().getOutputStream());
                String dataWrite1 = data;
                oos.writeObject(dataWrite1);
                oos.close();
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        close();
    }

    private synchronized String read()
    {
        String data = "";
        connect();
        try {
            if ((socketChannel != null) && socketChannel.isConnected()) {
                ois = new ObjectInputStream(socketChannel.socket().getInputStream());
                String dataRead1 = (String) ois.readObject();
                data = dataRead1;
                socketChannel.close();
            }
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        close();
        return data;
    }

    private synchronized void close()
    {
        try {
            if (socketChannel != null) socketChannel.close();
        } catch (IOException ex) {
            ex.printStackTrace();
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
    public boolean writeFile(FsPath path, InputStream inputStream, Long length)
            throws CacheException, InterruptedException, IOException,
                   URISyntaxException
    {
        boolean success = false;
        Subject subject = getSubject();
        subject = Subjects.ROOT;

        WriteTransfer transfer = new WriteTransfer(pnfsHandler, subject, path);
        _transfers.put((int) transfer.getSessionId(), transfer);
        try {
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
        return success;
    }

    public String getWriteUrl(FsPath path, Long length)
            throws CacheException, InterruptedException,
                   URISyntaxException
    {
        Subject subject = getSubject();

        String uri = null;
        WriteTransfer transfer = new WriteTransfer(pnfsHandler, subject, path);
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
     * @return
     * @throws diskCacheV111.util.CacheException
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     */
    public byte[] readFile(FsPath path, PnfsId pnfsid)
            throws CacheException, InterruptedException, IOException, URISyntaxException
    {
        byte[] result = null;
        ReadTransfer transfer = null;
        try {
            transfer = beginRead(path, pnfsid, true);
            result = transfer.relayData();
        } catch (CacheException e) {
            if (transfer != null) transfer.notifyBilling(e.getRc(), e.getMessage());
            throw e;
        } catch (InterruptedException e) {
            if (transfer != null) transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   "Transfer interrupted");
            throw e;
        } catch (IOException | URISyntaxException e) {
            if (transfer != null) transfer.notifyBilling(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                   e.toString());
            throw e;
        } finally {
            if (transfer != null) _transfers.remove((int) transfer.getSessionId());
        }
        return result;
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
        subject = Subjects.ROOT;

        String uri = null;
        ReadTransfer transfer = new ReadTransfer(pnfsHandler, subject, path, pnfsid);
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
        transfer.setPoolManagerStub(poolMgrStub);
        transfer.setPoolStub(poolStub);
        transfer.setBillingStub(billingStub);
        //transfer.setClientAddress(new InetSocketAddress(Subjects
        //        .getOrigin(subject).getAddress(),
        //        PROTOCOL_INFO_UNKNOWN_PORT));
        transfer.setClientAddress(new InetSocketAddress(_internalAddress,
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

        public byte[] relayData()
            throws IOException, CacheException, InterruptedException
        {
            byte[] result = null;
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
                        result = ByteStreams.toByteArray(inputStream);
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
            return result;
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

    private Set<FileAttribute> buildRequestedAttributes()
    {
        Set<FileAttribute> attributes = EnumSet.copyOf(REQUIRED_ATTRIBUTES);
        /*
        if (isDigestRequested()) {
            attributes.add(CHECKSUM);
        }
        */
        return attributes;
    }


    /**
     * Returns the resource object for a path.
     *
     * @param path The full path
     * @return
     */
    public int getResource(FsPath path)
    {
        int result = PROVIDER_ERROR;
        int tries = 0;
        Subject subject = getSubject();
        try {
            do {
                try {
                    PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                    Set<FileAttribute> requestedAttributes = buildRequestedAttributes();
                    _attributes = pnfs.getFileAttributes(path.toString(), requestedAttributes);
                    result = 0;
                } catch (FileNotFoundCacheException e) {
                    result = NOTFOUND_ERROR;
                }
                tries = tries + 1;
            } while ((result > 0) && (tries < maxTries));
        } catch (PermissionDeniedCacheException e) {
            result = PERMISSION_ERROR;
        } catch (CacheException e) {
            result = PROVIDER_ERROR;
        }
        return result;
    }

}
