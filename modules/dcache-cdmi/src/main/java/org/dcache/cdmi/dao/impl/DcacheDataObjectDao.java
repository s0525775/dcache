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
import com.google.common.io.ByteStreams;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
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
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.NoRouteToCellException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AceFlags;
import org.dcache.auth.Subjects;
import org.dcache.cdmi.exception.MethodNotAllowedException;
import org.dcache.cdmi.filter.ContextHolder;
import org.dcache.cdmi.model.DcacheDataObject;
import org.dcache.cdmi.util.AceConverter;
import org.dcache.cdmi.util.IdConverter;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.*;
import org.dcache.util.PingMoversTask;
import org.dcache.util.RedirectedTransfer;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;
import org.dcache.util.TransferRetryPolicy;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.dao.DataObjectDao;
import org.snia.cdmiserver.exception.BadRequestException;
import org.snia.cdmiserver.exception.ConflictException;
import org.snia.cdmiserver.exception.ForbiddenException;
import org.snia.cdmiserver.exception.UnauthorizedException;
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

    // Properties and Dependency Injection Methods by CDMI
    private String baseDirectoryName = null;
    private File baseDirectory = null;

    // Properties and Dependency Injection Methods by dCache
    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES =
        EnumSet.of(PNFSID, CREATION_TIME, ACCESS_TIME, CHANGE_TIME, MODIFICATION_TIME, TYPE, SIZE, MODE, ACL, OWNER, OWNER_GROUP, STORAGEINFO);
    private static final String PROTOCOL_INFO_NAME = "Http";
    private static final int PROTOCOL_INFO_MAJOR_VERSION = 1;
    private static final int PROTOCOL_INFO_MINOR_VERSION = 1;
    private static final int PROTOCOL_INFO_UNKNOWN_PORT = 0;
    private static final long PING_DELAY = 300000;
    private final Map<Integer,HttpTransfer> transfers = Maps.newConcurrentMap();
    private ScheduledExecutorService executor;
    private CellStub pnfsStub;
    private PnfsHandler pnfsHandler;
    private ListDirectoryHandler listDirectoryHandler;
    private CellStub poolStub;
    private CellStub poolMgrStub;
    private CellStub billingStub;
    private int moverTimeout = 180000;
    private TimeUnit moverTimeoutUnit = MILLISECONDS;
    private long killTimeout = 1500;
    private TimeUnit killTimeoutUnit = MILLISECONDS;
    private long transferConfirmationTimeout = 60000;
    private TimeUnit transferConfirmationTimeoutUnit = MILLISECONDS;
    private String ioQueue;
    private InetAddress internalAddress;
    private boolean isOverwriteAllowed;
    private boolean isAnonymousListingAllowed;
    private String realm = "dCache";
    private TransferRetryPolicy retryPolicy =
        TransferRetryPolicies.tryOncePolicy(moverTimeout, moverTimeoutUnit);
    private List<FsPath> _allowedPaths =
        Collections.singletonList(new FsPath());

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

    /**
     * <p>
     * Set AnonymousListing for dCache.
     * </p>
     *
     * @param isAllowed
     */
    public void setAnonymousListing(boolean isAllowed)
    {
        this.isAnonymousListingAllowed = isAllowed;
    }

    /**
     * <p>
     * Returns the internal Host address.
     * </p>
     *
     * @return
     */
    private String getInternalAddress()
    {
        return this.internalAddress.getHostAddress();
    }

    /**
     * Message handler for redirect messages from the pools.
     * @param envelope
     * @param message
     */
    public void messageArrived(CellMessage envelope,
                               HttpDoorUrlInfoMessage message)
    {
        HttpTransfer transfer = transfers.get((int) message.getId());
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
        Transfer transfer = transfers.get((int) message.getId());
        if (transfer != null) {
            transfer.finished(message);
        }
    }

    /**
     * Set the list of paths for which we allow access. Paths are
     * separated by a colon. This paths are relative to the root path.
     */
    public void setAllowedPaths(String s)
    {
        List<FsPath> list = new ArrayList<>();
        for (String path: s.split(":")) {
            list.add(new FsPath(path));
        }
        _allowedPaths = list;
    }

    /**
     * Returns true if access to path is allowed through the CDMI
     * door, false otherwise.
     */
    private boolean isAllowedPath(FsPath path)
    {
        for (FsPath allowedPath: _allowedPaths) {
            if (path.startsWith(allowedPath)) {
                return true;
            }
        }
        return false;
    }

    public DcacheDataObjectDao()
    {
        try {
            this.internalAddress = InetAddress.getLocalHost();
        } catch (UnknownHostException ex) {
            _log.error(realm);
        }
    }

    /**
     * Sets the ScheduledExecutorService used for periodic tasks.
     * @param executor
     */
    public void setExecutor(ScheduledExecutorService executor)
    {
        this.executor = executor;
        executor.scheduleAtFixedRate(new PingMoversTask<>(transfers.values()),
                                     PING_DELAY, PING_DELAY,
                                     MILLISECONDS);
    }

    /**
     * Returns the current Subject of the calling thread.
     */
    private static Subject getSubject()
    {
        return Subject.getSubject(ContextHolder.get());
    }

    /**
     * Returns the location URI of the current request. This is the
     * full request URI excluding user information, query and fragments.
     */
    private URI getLocation() throws URISyntaxException
    {
        URI uri = new URI("http://" + internalAddress.getHostName() + ":8543");
        return new URI(uri.getScheme(), null, uri.getHost(),
                uri.getPort(), uri.getPath(), null, null);
    }

    /**
     * <p>
     * Gets the ContainerName for a requested path.
     * </p>
     *
     * @param path
     */
    private String getcontainerName(String path)
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

    // DataObjectDao Methods invoked from PathResource
    @Override
    public DcacheDataObject createByPath(String path, DataObject dObj) throws CacheException
    {
        boolean objectIdPath = false;
        _log.trace("In DCacheDataObjectDao.createByPath, Path={}", path);

        if (path != null) {
            // ISO-8601 Date
            Date now = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            File objFile, baseDir, containerDirectory;
            String containerName = getcontainerName(path);
            DcacheDataObject newDObj = (DcacheDataObject) dObj;

            _log.trace("baseDirectory={}", baseDirectoryName);
            baseDir = new File(baseDirectoryName + "/");
            _log.trace("Base Directory AbsolutePath={}", baseDir.getAbsolutePath());
            containerDirectory = absoluteFile(containerName);
            _log.trace("Container AbsolutePath={}", containerDirectory.getAbsolutePath());
            objFile = absoluteFile(path);
            _log.trace("Object AbsolutePath={}", objFile.getAbsolutePath());

            Subject subject = getSubject();
            if ((subject == null) || Subjects.isNobody(subject)) {
                throw new ForbiddenException("Permission denied");
            }

            String checkPath = path;
            if (path.startsWith("cdmi_objectid/") || path.startsWith("/cdmi_objectid/")) {
                String objectId = "";
                String restPath = "";
                String tempPath = "";
                IdConverter idc = new IdConverter();
                if (path.startsWith("cdmi_objectid/")) {
                    tempPath = path.replace("cdmi_objectid/", "");
                } else {
                    tempPath = path.replace("/cdmi_objectid/", "");
                }
                int slashIndex = tempPath.indexOf("/");
                if (slashIndex > 0) {
                    objectId = tempPath.substring(0, slashIndex);
                    restPath = tempPath.substring(slashIndex + 1);
                    String strPnfsId = idc.toPnfsID(objectId);
                    PnfsId pnfsId = new PnfsId(strPnfsId);
                    FsPath pnfsPath = getPnfsPath(subject, pnfsId);
                    if ((restPath == null) || restPath.isEmpty() || (restPath.equals("/"))) {
                        objectIdPath = true;
                    }
                    if (pnfsPath != null) {
                        String strPnfsPath = removeSlashesFromPath(pnfsPath.toString());
                        if (strPnfsPath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                            String tmpBasePath = strPnfsPath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                            checkPath = "/" + tmpBasePath + "/" + removeSlashesFromPath(restPath);
                        }
                    }
                } else {
                    objectId = tempPath;
                    String strPnfsId = idc.toPnfsID(objectId);
                    PnfsId pnfsId = new PnfsId(strPnfsId);
                    FsPath pnfsPath = getPnfsPath(subject, pnfsId);
                    objectIdPath = true;
                    if (pnfsPath != null) {
                        String strPnfsPath = removeSlashesFromPath(pnfsPath.toString());
                        if (strPnfsPath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                            String tmpBasePath = strPnfsPath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                            checkPath = "/" + tmpBasePath;
                        }
                    }
                }
            }
            _log.trace("CheckPath={}", String.valueOf(checkPath));

            if (checkPath != null) {
                containerName = getcontainerName(checkPath);
                objFile = absoluteFile(checkPath);
                containerDirectory = absoluteFile(containerName);

                FsPath tmpFsPath = new FsPath();
                tmpFsPath.add(objFile.getAbsolutePath());
                if (!isAllowedPath(tmpFsPath)) {
                    throw new ForbiddenException("Permission denied");
                }

                if (newDObj.getMove() == null) { // This is a normal Create or Update

                    if (!checkIfDirectoryFileExists(subject, objFile.getAbsolutePath())) {  //Create
                        _log.trace("<DataObject Create>");

                        if (objectIdPath) {
                            throw new MethodNotAllowedException("This method is not supported yet.");
                        }

                        String base =  removeSlashesFromPath(baseDirectoryName);
                        String parent = removeSlashesFromPath(getParentDirectory(objFile.getAbsolutePath()));
                        if (!base.equals(parent)) {
                            if (!isUserAllowed(subject, parent)) {
                                throw new UnauthorizedException("Access denied", realm);
                            }
                        }
                        // check for container
                        if (!checkIfDirectoryFileExists(subject, containerDirectory.getAbsolutePath())) {
                            throw new BadRequestException("Container <"
                                                        + containerDirectory.getAbsolutePath()
                                                        + "> doesn't exist");
                        }
                        if (checkIfDirectoryFileExists(subject, objFile.getAbsolutePath())) {
                            throw new ConflictException("Object File <" + objFile.getAbsolutePath() + "> exists");
                        }

                        try {
                            FsPath fsPath = new FsPath(objFile.getAbsolutePath());
                            byte[] bytData = newDObj.getValue().getBytes(StandardCharsets.UTF_8);
                            InputStream isData = new ByteArrayInputStream(bytData);
                            if (!writeFile(subject, fsPath, isData, (long) bytData.length)) {
                                _log.error("Exception while writing dataobject.");
                                throw new BadRequestException("Cannot write Object file @" + path);
                            }
                        } catch (IllegalArgumentException | InterruptedException | IOException | URISyntaxException | CacheException ex) {
                            _log.error("Exception while writing dataobject, {}", ex);
                            throw new BadRequestException("Cannot write Object @" + path + ", error: " + ex);
                        }

                        FileAttributes attributes = getAttributes(subject, objFile.getAbsolutePath());

                        PnfsId pnfsId = null;
                        String objectID = "";
                        ACL oacl = null;
                        if (attributes != null) {
                            pnfsId = attributes.getPnfsId();
                            if (pnfsId != null) {
                                newDObj.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                                newDObj.setMetadata("cdmi_atime", sdf.format(attributes.getCreationTime()));
                                newDObj.setMetadata("cdmi_mtime", sdf.format(attributes.getCreationTime()));
                                newDObj.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                                newDObj.setMetadata("cdmi_owner", String.valueOf(attributes.getOwner()));
                                oacl = attributes.getAcl();
                                objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                                newDObj.setObjectID(objectID);
                                _log.trace("DCacheDataObjectDao<Create>, setObjectID={}", objectID);

                                String parentPath = "";
                                PnfsId parentPnfsId = getPnfsIDByPath(subject, parent);
                                String parentObjectId = new IdConverter().toObjectID(parentPnfsId.toIdString());
                                if (parent.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                    parentPath = parent.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                                } else {
                                    parentPath = parent;
                                }
                                newDObj.setParentID(parentObjectId);
                                newDObj.setParentURI(addSuffixSlashToPath(removeSlashesFromPath(parentPath)));

                                if (newDObj.getDomainURI() == null) {
                                    newDObj.setDomainURI("/cdmi_domains/default_domain");
                                }
                                newDObj.setObjectName(getItem(objFile.getAbsolutePath()));
                                newDObj.setObjectType("application/cdmi-object");
                                newDObj.setCapabilitiesURI("/cdmi_capabilities/dataobject");
                                newDObj.setMetadata("cdmi_acount", "0");
                                newDObj.setMetadata("cdmi_mcount", "0");
                                if (oacl != null && !oacl.isEmpty()) {
                                    ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                    for (ACE ace : oacl.getList()) {
                                        AceConverter ac = new AceConverter();
                                        HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                        subMetadataEntry_ACL.put("acetype", ac.convertToCdmiAceType(ace.getType().name()));
                                        subMetadataEntry_ACL.put("identifier", ac.convertToCdmiAceWho(ace.getWho().name()));
                                        subMetadataEntry_ACL.put("aceflags", ac.convertToCdmiAceFlags(AceFlags.asString(ace.getFlags())));
                                        subMetadataEntry_ACL.put("acemask", ac.convertToCdmiAceMask(AccessMask.asString(ace.getAccessMsk())));
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
                                String fileName = "";
                                String filePath = objFile.getAbsolutePath();
                                if (filePath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                    fileName = filePath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                                } else {
                                    fileName = filePath;
                                }
                                newDObj.setMetadata("fileName", fileName);

                                newDObj.setCompletionStatus("Complete");
                                return newDObj;
                            } else {
                                throw new BadRequestException("Error while creating dataobject '" + path + "', no PnfsId set.");
                            }
                        } else {
                            throw new BadRequestException("Error while creating dataobject '" + path + "', no attributes available.");
                        }

                    } else {  //Update
                        _log.trace("<DataObject Update>");

                        // check for container
                        if (!checkIfDirectoryFileExists(subject, containerDirectory.getAbsolutePath())) {
                            throw new BadRequestException("Container <"
                                                        + containerDirectory.getAbsolutePath()
                                                        + "> doesn't exist");
                        }
                        if (!checkIfDirectoryFileExists(subject, objFile.getAbsolutePath())) {
                            throw new BadRequestException("Object File <" + objFile.getAbsolutePath() + "> does not exist");
                        }

                        try {
                            DcacheDataObject currentDObj = new DcacheDataObject();
                            FileAttributes attributes = getAttributes(subject, objFile.getAbsolutePath());

                            PnfsId pnfsId = null;
                            String objectId = "";
                            ACL oacl = null;
                            if (attributes != null) {
                                pnfsId = attributes.getPnfsId();
                                if (pnfsId != null) {
                                    // Read object from file
                                    if (attributes.getFileType() != DIR) {
                                        pnfsId = attributes.getPnfsId();
                                        byte[] inBytes = readFile(subject, new FsPath(objFile.getAbsolutePath()), pnfsId);
                                        if (inBytes != null) {
                                            currentDObj.setValue(new String(inBytes, "UTF-8"));
                                        }
                                    }
                                    currentDObj.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                                    currentDObj.setMetadata("cdmi_atime", sdf.format(attributes.getCreationTime()));
                                    currentDObj.setMetadata("cdmi_mtime", sdf.format(attributes.getCreationTime()));
                                    currentDObj.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                                    currentDObj.setMetadata("cdmi_owner", String.valueOf(attributes.getOwner()));
                                    currentDObj.setValueRange("0-" + String.valueOf(attributes.getSize() - 1));
                                    currentDObj.setValueTransferEncoding("utf-8");
                                    oacl = attributes.getAcl();
                                    objectId = new IdConverter().toObjectID(pnfsId.toIdString());
                                    currentDObj.setObjectID(objectId);
                                    _log.trace("DCacheDataObjectDao<Update>, setObjectID={}", objectId);

                                    if (newDObj.getDomainURI() == null) {
                                        currentDObj.setDomainURI("/cdmi_domains/default_domain");
                                    } else {
                                        currentDObj.setDomainURI(newDObj.getDomainURI());
                                    }
                                    currentDObj.setObjectName(getItem(objFile.getAbsolutePath()));
                                    currentDObj.setObjectType("application/cdmi-object");
                                    currentDObj.setCapabilitiesURI("/cdmi_capabilities/dataobject");
                                    currentDObj.setMetadata("cdmi_acount", "0");
                                    currentDObj.setMetadata("cdmi_mcount", "0");
                                    if (oacl != null && !oacl.isEmpty()) {
                                        ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                        for (ACE ace : oacl.getList()) {
                                            AceConverter ac = new AceConverter();
                                            HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                            subMetadataEntry_ACL.put("acetype", ac.convertToCdmiAceType(ace.getType().name()));
                                            subMetadataEntry_ACL.put("identifier", ac.convertToCdmiAceWho(ace.getWho().name()));
                                            subMetadataEntry_ACL.put("aceflags", ac.convertToCdmiAceFlags(AceFlags.asString(ace.getFlags())));
                                            subMetadataEntry_ACL.put("acemask", ac.convertToCdmiAceMask(AccessMask.asString(ace.getAccessMsk())));
                                            subMetadata_ACL.add(subMetadataEntry_ACL);
                                        }
                                        currentDObj.setSubMetadata_ACL(subMetadata_ACL);
                                    } else {
                                        ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                        HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                        subMetadataEntry_ACL.put("acetype", "ALLOW");
                                        subMetadataEntry_ACL.put("identifier", "OWNER@");
                                        subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                                        subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                                        subMetadata_ACL.add(subMetadataEntry_ACL);
                                        currentDObj.setSubMetadata_ACL(subMetadata_ACL);
                                    }

                                    if (!currentDObj.getValue().equals(newDObj.getValue())) {
                                        deleteFile(subject, pnfsId, objFile.getAbsolutePath());
                                        try {
                                            FsPath fsPath = new FsPath(objFile.getAbsolutePath());
                                            byte[] bytData = newDObj.getValue().getBytes(StandardCharsets.UTF_8);
                                            InputStream isData = new ByteArrayInputStream(bytData);
                                            if (!writeFile(subject, fsPath, isData, (long) bytData.length)) {
                                                _log.error("Exception while writing dataobject.");
                                                throw new BadRequestException("Cannot write Object file @" + path);
                                            }
                                        } catch (IllegalArgumentException | InterruptedException | IOException | URISyntaxException | CacheException ex) {
                                            _log.error("Exception while writing dataobject, {}", ex);
                                            throw new BadRequestException("Cannot write Object @" + path + ", error: " + ex);
                                        }
                                    }

                                    //forth-and-back update
                                    for (String key : newDObj.getMetadata().keySet()) {
                                        currentDObj.setMetadata(key, newDObj.getMetadata().get(key));
                                    }
                                    for (String key : currentDObj.getMetadata().keySet()) {
                                        newDObj.setMetadata(key, currentDObj.getMetadata().get(key));
                                    }

                                    attributes = getAttributes(subject, objFile.getAbsolutePath());

                                    pnfsId = null;
                                    String objectID = "";
                                    oacl = null;
                                    if (attributes != null) {
                                        pnfsId = attributes.getPnfsId();
                                        if (pnfsId != null) {
                                            newDObj.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                                            newDObj.setMetadata("cdmi_atime", sdf.format(attributes.getCreationTime()));
                                            newDObj.setMetadata("cdmi_mtime", sdf.format(attributes.getCreationTime()));
                                            newDObj.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                                            newDObj.setMetadata("cdmi_owner", String.valueOf(attributes.getOwner()));
                                            oacl = attributes.getAcl();
                                            objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                                            newDObj.setObjectID(objectID);
                                            _log.trace("DCacheDataObjectDao<Create>, setObjectID={}", objectID);

                                            String parentPath = "";
                                            String parent = removeSlashesFromPath(getParentDirectory(objFile.getAbsolutePath()));
                                            PnfsId parentPnfsId = getPnfsIDByPath(subject, parent);
                                            String parentObjectId = new IdConverter().toObjectID(parentPnfsId.toIdString());
                                            if (parent.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                                parentPath = parent.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                                            } else {
                                                parentPath = parent;
                                            }
                                            newDObj.setParentID(parentObjectId);
                                            newDObj.setParentURI(addSuffixSlashToPath(removeSlashesFromPath(parentPath)));

                                            newDObj.setObjectName(getItem(objFile.getAbsolutePath()));
                                            newDObj.setDomainURI(currentDObj.getDomainURI());
                                            newDObj.setObjectType("application/cdmi-object");
                                            newDObj.setCapabilitiesURI("/cdmi_capabilities/dataobject");
                                            newDObj.setMetadata("cdmi_acount", "0");
                                            newDObj.setMetadata("cdmi_mcount", "0");
                                            if (oacl != null && !oacl.isEmpty()) {
                                                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                                for (ACE ace : oacl.getList()) {
                                                    AceConverter ac = new AceConverter();
                                                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                                    subMetadataEntry_ACL.put("acetype", ac.convertToCdmiAceType(ace.getType().name()));
                                                    subMetadataEntry_ACL.put("identifier", ac.convertToCdmiAceWho(ace.getWho().name()));
                                                    subMetadataEntry_ACL.put("aceflags", ac.convertToCdmiAceFlags(AceFlags.asString(ace.getFlags())));
                                                    subMetadataEntry_ACL.put("acemask", ac.convertToCdmiAceMask(AccessMask.asString(ace.getAccessMsk())));
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
                                            String fileName = "";
                                            String filePath = objFile.getAbsolutePath();
                                            if (filePath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                                fileName = filePath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                                            } else {
                                                fileName = filePath;
                                            }
                                            newDObj.setMetadata("fileName", fileName);

                                            // update meta information
                                            try {
                                                FileAttributes attr = new FileAttributes();
                                                Date atime = sdf.parse(newDObj.getMetadata().get("cdmi_atime"));
                                                Date mtime = sdf.parse(newDObj.getMetadata().get("cdmi_mtime"));
                                                long atimeAsLong = atime.getTime();
                                                long mtimeAsLong = mtime.getTime();
                                                attr.setAccessTime(atimeAsLong);
                                                attr.setModificationTime(mtimeAsLong);
                                                PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                                                pnfs.setFileAttributes(pnfsId, attr);
                                            } catch (CacheException | ParseException ex) {
                                                ex.printStackTrace();
                                                _log.error("DcacheDataObjectDao<Update>, Cannot update meta information for object with objectID {}", newDObj.getObjectID());
                                            }
                                            newDObj.setCompletionStatus("Complete");
                                            return newDObj;
                                        } else {
                                            throw new BadRequestException("Error while creating dataobject '" + path + "', no PnfsId set.");
                                        }
                                    } else {
                                        throw new BadRequestException("Error while creating dataobject '" + path + "', no attributes available.");
                                    }
                                } else {
                                    throw new BadRequestException("Error while creating dataobject '" + path + "', no PnfsId set.");
                                }
                            } else {
                                throw new BadRequestException("Error while creating dataobject '" + path + "', no attributes available.");
                            }
                        } catch (CacheException | IOException | InterruptedException | URISyntaxException ex) {
                            _log.error("Exception while reading, {}", ex);
                            throw new BadRequestException("Cannot read Object @" + path + " error: " + ex);
                        } catch (UnauthorizedException ex) {
                            _log.error("Exception while reading, {}", ex);
                            throw new ForbiddenException("Permission denied");
                        }
                    }
                } else { // Moving/Renaming a DataObject
                    _log.trace("<DataObject Move>");
                    if (objectIdPath) {
                        throw new MethodNotAllowedException("This method is not supported yet.");
                    }

                    File sourceFile = absoluteFile(dObj.getMove());

                    if (!isUserAllowed(subject, sourceFile.getAbsolutePath())) {
                        throw new ForbiddenException("Permission denied");
                    }

                    // check for container
                    if (!checkIfDirectoryFileExists(subject, containerDirectory.getAbsolutePath())) {
                        throw new BadRequestException("Container <"
                                                    + containerDirectory.getAbsolutePath()
                                                    + "> doesn't exist");
                    }
                    if (!checkIfDirectoryFileExists(subject, sourceFile.getAbsolutePath())) {
                        throw new BadRequestException("Object File <" + objFile.getAbsolutePath() + "> does not exist");
                    }

                    String base =  removeSlashesFromPath(baseDirectoryName);
                    String parent = removeSlashesFromPath(getParentDirectory(objFile.getAbsolutePath()));

                    if (!checkIfDirectoryFileExists(subject, objFile.getAbsolutePath())) {
                        if (!base.equals(parent)) {
                            if (!isUserAllowed(subject, parent)) {
                                throw new ForbiddenException("Permission denied");
                            }
                        }
                    } else {
                        throw new ConflictException("Cannot move dataobject '" + dObj.getMove()
                                                       + "' to '" + path + "'; Destination already exists");
                    }

                    DcacheDataObject movedDObj = new DcacheDataObject();
                    FileAttributes attributes = renameFile(subject, sourceFile.getAbsolutePath(), objFile.getAbsolutePath());

                    PnfsId pnfsId = null;
                    String objectId = "";
                    ACL cacl = null;
                    if (attributes != null) {
                        pnfsId = attributes.getPnfsId();
                        if (pnfsId != null) {
                            for (String key : newDObj.getMetadata().keySet()) {
                                movedDObj.setMetadata(key, newDObj.getMetadata().get(key));
                            }
                            movedDObj.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                            movedDObj.setMetadata("cdmi_atime", sdf.format(attributes.getAccessTime()));
                            movedDObj.setMetadata("cdmi_mtime", sdf.format(attributes.getModificationTime()));
                            movedDObj.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                            movedDObj.setMetadata("cdmi_owner", String.valueOf(attributes.getOwner()));
                            cacl = attributes.getAcl();
                            objectId = new IdConverter().toObjectID(pnfsId.toIdString());
                            movedDObj.setObjectID(objectId);
                            _log.trace("DcacheContainerDao<Move>, setObjectID={}", objectId);

                            String parentPath = "";
                            String parent2 = removeSlashesFromPath(getParentDirectory(objFile.getAbsolutePath()));
                            PnfsId parentPnfsId = getPnfsIDByPath(subject, parent2);
                            String parentObjectId = new IdConverter().toObjectID(parentPnfsId.toIdString());
                            if (parent2.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                parentPath = parent2.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                            } else {
                                parentPath = parent2;
                            }
                            movedDObj.setParentID(parentObjectId);
                            movedDObj.setParentURI(addSuffixSlashToPath(removeSlashesFromPath(parentPath)));

                            if (newDObj.getDomainURI() == null) {
                                movedDObj.setDomainURI("/cdmi_domains/default_domain");
                            } else {
                                movedDObj.setDomainURI(newDObj.getDomainURI());
                            }
                            movedDObj.setObjectName(getItem(objFile.getAbsolutePath()));
                            movedDObj.setCapabilitiesURI("/cdmi_capabilities/container/default");
                            movedDObj.setMetadata("cdmi_ctime", sdf.format(now));
                            movedDObj.setMetadata("cdmi_atime", sdf.format(now));
                            movedDObj.setMetadata("cdmi_mtime", sdf.format(now));

                            if (cacl != null && !cacl.isEmpty()) {
                                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                for (ACE ace : cacl.getList()) {
                                    AceConverter ac = new AceConverter();
                                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                    subMetadataEntry_ACL.put("acetype", ac.convertToCdmiAceType(ace.getType().name()));
                                    subMetadataEntry_ACL.put("identifier", ac.convertToCdmiAceWho(ace.getWho().name()));
                                    subMetadataEntry_ACL.put("aceflags", ac.convertToCdmiAceFlags(AceFlags.asString(ace.getFlags())));
                                    subMetadataEntry_ACL.put("acemask", ac.convertToCdmiAceMask(AccessMask.asString(ace.getAccessMsk())));
                                    subMetadata_ACL.add(subMetadataEntry_ACL);
                                }
                                movedDObj.setSubMetadata_ACL(subMetadata_ACL);
                            } else {
                                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                subMetadataEntry_ACL.put("acetype", "ALLOW");
                                subMetadataEntry_ACL.put("identifier", "OWNER@");
                                subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                                subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                                subMetadata_ACL.add(subMetadataEntry_ACL);
                                movedDObj.setSubMetadata_ACL(subMetadata_ACL);
                            }

                            String mimeType = movedDObj.getMimetype();
                            if (mimeType == null) {
                                mimeType = "text/plain";
                            }
                            movedDObj.setMimetype(mimeType);
                            movedDObj.setMetadata("mimetype", mimeType);
                            String fileName = "";
                            String filePath = objFile.getAbsolutePath();
                            if (filePath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                fileName = filePath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                            } else {
                                fileName = filePath;
                            }
                            movedDObj.setMetadata("fileName", fileName);

                            // update meta information
                            try {
                                FileAttributes attr = new FileAttributes();
                                Date ctime = sdf.parse(movedDObj.getMetadata().get("cdmi_ctime"));
                                Date atime = sdf.parse(movedDObj.getMetadata().get("cdmi_atime"));
                                Date mtime = sdf.parse(movedDObj.getMetadata().get("cdmi_mtime"));
                                long ctimeAsLong = ctime.getTime();
                                long atimeAsLong = atime.getTime();
                                long mtimeAsLong = mtime.getTime();
                                attr.setCreationTime(ctimeAsLong);
                                attr.setAccessTime(atimeAsLong);
                                attr.setModificationTime(mtimeAsLong);
                                PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                                pnfs.setFileAttributes(pnfsId, attr);
                            } catch (CacheException | ParseException ex) {
                                _log.error("DcacheContainerDao<Move>, Cannot update meta information for object with objectID {}", movedDObj.getObjectID());
                            }
                            movedDObj.setCompletionStatus("Complete");
                            return newDObj;
                        } else {
                            throw new BadRequestException("Error while moving container '" + path + "', no PnfsId set.");
                        }
                    } else {
                        throw new BadRequestException("Error while moving container '" + path + "', no attributes available.");
                    }
                }
            } else {
                throw new BadRequestException("No path given");
            }
        } else {
            throw new BadRequestException("No path given");
        }
    }

    @Override
    public void deleteByPath(String path)
    {
        //please see DcacheContainerDao class for implementation
        throw new UnsupportedOperationException("DcacheDataObjectDao.deleteByPath()");
    }

    @Override
    public DcacheDataObject findByPath(String path)
    {
        _log.trace("In DCacheDataObjectDao.findByPath, Path={}", path);
        if (path != null) {
            try {
                // ISO-8601 Date
                Date now = new Date();
                long nowAsLong = now.getTime();
                DcacheDataObject dObj = new DcacheDataObject();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

                Subject subject = getSubject();
                if (!isAnonymousListingAllowed && (subject == null) && Subjects.isNobody(subject)) {
                    throw new UnauthorizedException("Access denied", realm);
                }

                // Check for object file
                File objFile, baseDirectory;
                _log.trace("baseDirectory={}", baseDirectoryName);
                baseDirectory = new File(baseDirectoryName + "/");
                String checkPath = path;
                if (path.startsWith("cdmi_objectid/") || path.startsWith("/cdmi_objectid/")) {
                    String objectId = "";
                    String restPath = "";
                    String tempPath = "";
                    IdConverter idc = new IdConverter();
                    if (path.startsWith("cdmi_objectid/")) {
                        tempPath = path.replace("cdmi_objectid/", "");
                    } else {
                        tempPath = path.replace("/cdmi_objectid/", "");
                    }
                    int slashIndex = tempPath.indexOf("/");
                    if (slashIndex > 0) {
                        objectId = tempPath.substring(0, slashIndex);
                        restPath = tempPath.substring(slashIndex + 1);
                        String strPnfsId = idc.toPnfsID(objectId);
                        PnfsId pnfsId = new PnfsId(strPnfsId);
                        FsPath pnfsPath = getPnfsPath(subject, pnfsId);
                        if (pnfsPath != null) {
                            String strPnfsPath = removeSlashesFromPath(pnfsPath.toString());
                            if (strPnfsPath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                String tmpBasePath = strPnfsPath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                                checkPath = "/" + tmpBasePath + "/" + removeSlashesFromPath(restPath);
                            }
                        }
                    } else {
                        objectId = tempPath;
                        String strPnfsId = idc.toPnfsID(objectId);
                        PnfsId pnfsId = new PnfsId(strPnfsId);
                        FsPath pnfsPath = getPnfsPath(subject, pnfsId);
                        if (pnfsPath != null) {
                            String strPnfsPath = removeSlashesFromPath(pnfsPath.toString());
                            if (strPnfsPath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                String tmpBasePath = strPnfsPath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                                checkPath = "/" + tmpBasePath;
                            }
                        }
                    }
                }
                _log.trace("CheckPath={}", String.valueOf(checkPath));

                if (checkPath != null) {
                    objFile = new File(baseDirectory, checkPath);

                    FsPath fsPath = new FsPath();
                    fsPath.add(objFile.getAbsolutePath());
                    if (!isAllowedPath(fsPath)) {
                        throw new ForbiddenException("Permission denied");
                    }

                    if (!checkIfDirectoryFileExists(subject, objFile.getAbsolutePath())) {
                        return null;
                    } else {
                        if (!isUserAllowed(subject, objFile.getAbsolutePath())) {
                            throw new UnauthorizedException("Access denied", realm);
                        }
                    }

                    PnfsId pnfsId = null;
                    FileAttributes attributes = getAttributes(subject, objFile.getAbsolutePath());
                    String objectID = "";
                    int oowner = 0;
                    ACL oacl = null;
                    if (attributes != null) {
                        // Read object from file
                        if (attributes.getFileType() != DIR) {
                            pnfsId = attributes.getPnfsId();
                            byte[] inBytes = readFile(subject, new FsPath(objFile.getAbsolutePath()), pnfsId);
                            if (inBytes != null) {
                                dObj.setValue(new String(inBytes, "UTF-8"));
                            }
                        }
                        pnfsId = attributes.getPnfsId();
                        if (pnfsId != null) {
                            long ctime = attributes.getCreationTime();
                            long atime = attributes.getAccessTime();
                            long mtime = attributes.getModificationTime();
                            long osize = attributes.getSize();
                            dObj.setMetadata("cdmi_ctime", sdf.format(ctime));
                            dObj.setMetadata("cdmi_atime", sdf.format(atime));
                            dObj.setMetadata("cdmi_mtime", sdf.format(mtime));
                            dObj.setMetadata("cdmi_size", String.valueOf(osize));
                            dObj.setValueRange("0-" + String.valueOf(osize - 1));
                            dObj.setValueTransferEncoding("utf-8");
                            oowner = attributes.getOwner();
                            oacl = attributes.getAcl();
                            objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                            dObj.setObjectID(objectID);
                            _log.trace("DCacheDataObjectDao<Read>, setObjectID={}", objectID);
                        } else {
                            _log.error("DCacheDataObjectDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
                        }
                    } else {
                        _log.error("DCacheDataObjectDao<Read>, Cannot read meta information from object {}", objFile.getAbsolutePath());
                    }

                    if (dObj.getDomainURI() == null) {
                        dObj.setDomainURI("/cdmi_domains/default_domain");
                    }
                    dObj.setObjectName(getItem(objFile.getAbsolutePath()));
                    dObj.setObjectType("application/cdmi-object");
                    dObj.setCapabilitiesURI("/cdmi_capabilities/dataobject");
                    dObj.setMetadata("cdmi_acount", "0");
                    dObj.setMetadata("cdmi_mcount", "0");
                    dObj.setMetadata("cdmi_owner", String.valueOf(oowner));  //TODO: need to implement authentication and autorization first
                    if (oacl != null && !oacl.isEmpty()) {
                        ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                        for (ACE ace : oacl.getList()) {
                            AceConverter ac = new AceConverter();
                            HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                            subMetadataEntry_ACL.put("acetype", ac.convertToCdmiAceType(ace.getType().name()));
                            subMetadataEntry_ACL.put("identifier", ac.convertToCdmiAceWho(ace.getWho().name()));
                            subMetadataEntry_ACL.put("aceflags", ac.convertToCdmiAceFlags(AceFlags.asString(ace.getFlags())));
                            subMetadataEntry_ACL.put("acemask", ac.convertToCdmiAceMask(AccessMask.asString(ace.getAccessMsk())));
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

                    String parentPath = "";
                    String parent = removeSlashesFromPath(getParentDirectory(objFile.getAbsolutePath()));
                    PnfsId parentPnfsId = getPnfsIDByPath(subject, parent);
                    String parentObjectId = new IdConverter().toObjectID(parentPnfsId.toIdString());
                    if (parent.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                        parentPath = parent.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                    } else {
                        parentPath = parent;
                    }
                    dObj.setParentID(parentObjectId);
                    dObj.setParentURI(addSuffixSlashToPath(removeSlashesFromPath(parentPath)));

                    String mimeType = dObj.getMimetype();
                    if (mimeType == null) {
                        mimeType = "text/plain";
                    }
                    dObj.setMimetype(mimeType);
                    dObj.setMetadata("mimetype", mimeType);
                    String fileName = "";
                    String filePath = objFile.getAbsolutePath();
                    if (filePath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                        fileName = filePath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                    } else {
                        fileName = filePath;
                    }
                    dObj.setMetadata("fileName", fileName);

                    FileAttributes attr = new FileAttributes();
                    attr.setAccessTime(nowAsLong);
                    PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                    pnfs.setFileAttributes(pnfsId, attr);
                    dObj.setMetadata("cdmi_atime", sdf.format(now));
                    dObj.setCompletionStatus("Complete");
                    return dObj;
                } else {
                    throw new BadRequestException("No path given.");
                }
            } catch (CacheException | IOException | InterruptedException | URISyntaxException ex) {
                _log.error("Exception while reading, {}", ex);
                throw new BadRequestException("Cannot read Object @" + path + " error: " + ex);
            } catch (UnauthorizedException ex) {
                _log.error("Exception while reading, {}", ex);
                throw new ForbiddenException("Permission denied");
            }
        } else {
            throw new BadRequestException("No path given.");
        }
    }

    @Override
    public DcacheDataObject findByObjectId(String objectId)
    {
        _log.trace("In DCacheDataObjectDao.findByObjectId, ObjectID={}", removeSlashesFromPath(objectId));
        if (objectId != null) {
            try {
                // ISO-8601 Date
                Date now = new Date();
                long nowAsLong = now.getTime();
                DcacheDataObject dObj = new DcacheDataObject();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

                IdConverter idconverter = new IdConverter();
                String strPnfsId = idconverter.toPnfsID(removeSlashesFromPath(objectId));
                PnfsId pnfsId = new PnfsId(strPnfsId);

                Subject subject = getSubject();
                if (!isAnonymousListingAllowed && (subject == null) && Subjects.isNobody(subject)) {
                    throw new UnauthorizedException("Access denied", realm);
                }

                if (!checkIfDirectoryFileExists(subject, pnfsId)) {
                    return null;
                } else {
                    if (!isUserAllowed(subject, pnfsId)) {
                        throw new UnauthorizedException("Access denied", realm);
                    }
                }

                String path = "";
                FileAttributes attributes = getAttributes(subject, pnfsId);
                String objectID = "";
                int oowner = 0;
                ACL oacl = null;
                if (attributes != null) {
                    if (attributes.getFileType() != DIR) {
                        // Read object from file
                        byte[] inBytes = readFile(subject, new FsPath(path), pnfsId);
                        if (inBytes != null) {
                            dObj.setValue(new String(inBytes, "UTF-8"));
                        }
                        pnfsId = attributes.getPnfsId();
                        if (pnfsId != null) {
                            path = getPnfsPath(subject, pnfsId).toString();
                            long ctime = attributes.getCreationTime();
                            long atime = attributes.getAccessTime();
                            long mtime = attributes.getModificationTime();
                            long osize = attributes.getSize();
                            dObj.setMetadata("cdmi_ctime", sdf.format(ctime));
                            dObj.setMetadata("cdmi_atime", sdf.format(atime));
                            dObj.setMetadata("cdmi_mtime", sdf.format(mtime));
                            dObj.setMetadata("cdmi_size", String.valueOf(osize));
                            dObj.setValueRange("0-" + String.valueOf(osize - 1));
                            dObj.setValueTransferEncoding("utf-8");
                            oowner = attributes.getOwner();
                            oacl = attributes.getAcl();
                            objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                            dObj.setObjectID(objectID);

                            if (dObj.getDomainURI() == null) {
                                dObj.setDomainURI("/cdmi_domains/default_domain");
                            }
                            dObj.setObjectName(getItem(path));
                            dObj.setObjectType("application/cdmi-object");
                            dObj.setCapabilitiesURI("/cdmi_capabilities/dataobject");
                            dObj.setMetadata("cdmi_acount", "0");
                            dObj.setMetadata("cdmi_mcount", "0");
                            dObj.setMetadata("cdmi_owner", String.valueOf(oowner));
                            dObj.setValueRange("0-" + String.valueOf(osize - 1));
                            dObj.setValueTransferEncoding("ascii");
                            if (oacl != null && !oacl.isEmpty()) {
                                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                for (ACE ace : oacl.getList()) {
                                    AceConverter ac = new AceConverter();
                                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                    subMetadataEntry_ACL.put("acetype", ac.convertToCdmiAceType(ace.getType().name()));
                                    subMetadataEntry_ACL.put("identifier", ac.convertToCdmiAceWho(ace.getWho().name()));
                                    subMetadataEntry_ACL.put("aceflags", ac.convertToCdmiAceFlags(AceFlags.asString(ace.getFlags())));
                                    subMetadataEntry_ACL.put("acemask", ac.convertToCdmiAceMask(AccessMask.asString(ace.getAccessMsk())));
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

                            String parentPath = "";
                            String parent = removeSlashesFromPath(getParentDirectory(path));
                            PnfsId parentPnfsId = getPnfsIDByPath(subject, parent);
                            String parentObjectId = new IdConverter().toObjectID(parentPnfsId.toIdString());
                            if (parent.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                parentPath = parent.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                            } else {
                                parentPath = parent;
                            }
                            dObj.setParentID(parentObjectId);
                            dObj.setParentURI(addSuffixSlashToPath(removeSlashesFromPath(parentPath)));

                            String mimeType = dObj.getMimetype();
                            if (mimeType == null) {
                                mimeType = "text/plain";
                            }
                            dObj.setMimetype(mimeType);
                            dObj.setMetadata("mimetype", mimeType);
                            String fileName = "";
                            String filePath = path;
                            if (filePath.contains(removeSlashesFromPath(baseDirectoryName) + "/")) {
                                fileName = filePath.replace(removeSlashesFromPath(baseDirectoryName) + "/", "");
                            } else {
                                fileName = filePath;
                            }
                            dObj.setMetadata("fileName", fileName);

                            FileAttributes attr = new FileAttributes();
                            attr.setAccessTime(nowAsLong);
                            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                            pnfs.setFileAttributes(pnfsId, attr);
                            dObj.setMetadata("cdmi_atime", sdf.format(now));
                            dObj.setCompletionStatus("Complete");
                            return dObj;
                        } else {
                            throw new BadRequestException("Exception while reading dataobject with objectId " + removeSlashesFromPath(objectId)
                                        + ", metadata could not become read");
                        }
                    } else {
                        throw new BadRequestException("Exception while reading dataobject with objectId " + removeSlashesFromPath(objectId)
                                    + ", object is a directory");
                    }
                } else {
                    throw new BadRequestException("Exception while reading dataobject with objectId " + removeSlashesFromPath(objectId)
                                + ", metadata could not become read");
                }
            } catch (UnauthorizedException ex) {
                throw new UnauthorizedException("Access denied", realm);
            } catch (CacheException | IOException | InterruptedException | URISyntaxException ex) {
                throw new BadRequestException("Exception while reading dataobject with objectId " + removeSlashesFromPath(objectId));
            }
        } else {
            throw new BadRequestException("No path given.");
        }
    }

    @Override
    public DcacheDataObject createById(String string, DataObject d)
    {
        //This method is not supported by dCache, dCache would have to allow a user-defined PnfsID
        throw new MethodNotAllowedException("Method Not Allowed"); //To change body of generated methods, choose Tools | Templates.
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
     * Return a {@link File} instance for the file or directory at the specified path from our base
     * directory.
     * </p>
     *
     * @param path
     *            Path of the requested file or directory.
     * @return
     */
    private File absoluteFile(String path)
    {
        if (path == null) {
            return baseDirectory();
        } else {
            return new File(baseDirectory(), path);
        }
    }

    /**
     * <p>
     * Return a {@link File} instance for the base directory.
     * </p>
     */
    private File baseDirectory()  //TODO!!!
    {
        if (baseDirectory == null) {
            baseDirectory = new File(baseDirectoryName);
        }
        return baseDirectory;
    }

    /**
     * <p>
     * Checks if a user is allowed to access a file path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private boolean isUserAllowed(Subject subject, String path)
    {
        boolean result = false;
        try {
            String tmpPath = addPrefixSlashToPath(path);
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            FileAttributes attr = pnfs.getFileAttributes(tmpPath, REQUIRED_ATTRIBUTES);
            if (Subjects.getUid(subject) == attr.getOwner()) {
                result = true;
            }
        } catch (PermissionDeniedCacheException e) {
            return false;
        } catch (CacheException ignore) {
            return true;
        }
        return result;
    }

    /**
     * <p>
     * Checks if a user is allowed to access a file path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private boolean isUserAllowed(Subject subject, PnfsId pnfsid)
    {
        boolean result = false;
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            FileAttributes attr = pnfs.getFileAttributes(pnfsid, REQUIRED_ATTRIBUTES);
            if (Subjects.getUid(subject) == attr.getOwner()) {
                result = true;
            }
        } catch (PermissionDeniedCacheException e) {
            return false;
        } catch (CacheException ignore) {
            return true;
        }
        return result;
    }

    /**
     * <p>
     * Get the PnfsId of a path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private PnfsId getPnfsIDByPath(Subject subject, String path)
    {
        PnfsId result = null;
        try {
            String tmpPath = addPrefixSlashToPath(path);
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            FileAttributes attr = pnfs.getFileAttributes(tmpPath, REQUIRED_ATTRIBUTES);
            if (attr.getFileType() == DIR) {
                result = attr.getPnfsId();
            }
        } catch (CacheException ignore) {
        }
        return result;
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
     * Gets the item of a file path.
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
    private synchronized boolean checkIfDirectoryFileExists(Subject subject, String dirPath)
    {
        boolean result = false;
        try {
            String tmpDirPath = addPrefixSlashToPath(dirPath);
            System.out.println(tmpDirPath);
            PnfsId check = null;
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            check = pnfs.getPnfsIdByPath(tmpDirPath);
            if (check != null) {
                result = true;
            }
        } catch (CacheException ignore) {
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
    private synchronized boolean checkIfDirectoryFileExists(Subject subject, PnfsId pnfsid)
    {
        boolean result = false;
        try {
            FsPath check = null;
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            check = pnfs.getPathByPnfsId(pnfsid);
            if (check != null) {
                result = true;
            }
        } catch (CacheException ignore) {
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
    private FileAttributes getAttributes(Subject subject, String path)
    {
        FileAttributes result = null;
        try {
            String tmpDirPath = addPrefixSlashToPath(path);
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            result = pnfs.getFileAttributes(new FsPath(tmpDirPath), REQUIRED_ATTRIBUTES);
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException("Permission denied");
        } catch (CacheException ex) {
            _log.error("DcacheDataObjectDao<getAttributes>, Could not retreive attributes for path {}", path);
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
    private FileAttributes getAttributes(Subject subject, PnfsId pnfsid)
    {
        FileAttributes result = null;
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            result = pnfs.getFileAttributes(pnfsid, REQUIRED_ATTRIBUTES);
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException("Permission denied");
        } catch (CacheException ex) {
            _log.error("DcacheDataObjectDao<getAttributes>, Could not retreive attributes for PnfsId {}", pnfsid);
        }
        return result;
    }

    /**
     * <p>
     * Retrieves the PnfsPath for a specific PnfsId.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private FsPath getPnfsPath(Subject subject, PnfsId pnfsid)
    {
        FsPath result = null;
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            result = pnfs.getPathByPnfsId(pnfsid);
        } catch (CacheException ex) {
            _log.warn("PnfsPath for PnfsId '{}' could not get retrieved, {}", pnfsid, ex.getMessage());
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

    /**
     * <p>
     * Adds a suffix slash to a directory path if there isn't a slash already.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private String addSuffixSlashToPath(String path)
    {
        String result = "";
        if (path != null && path.length() > 0) {
            if (!path.endsWith("/")) {
                result = path + "/";
            } else {
                result = path;
            }
        }
        return result;
    }

    /**
     * <p>
     * Removes prefix and suffix slashes from a directory path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private String removeSlashesFromPath(String path)
    {
        String result = "";
        if (path != null && path.length() > 0) {
            if (path.startsWith("/")) {
                result = path.substring(1);
            } else {
                result = path;
            }
            if (result.endsWith("/")) {
                result = result.substring(0, result.length()-1);
            }
        }
        return result;
    }

    /**
     * <p>
     * Creates a Directory in a specific file path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private FileAttributes renameFile(Subject subject, String fromPath, String toPath)
    {
        FileAttributes result = null;
        String tmpFromPath = addPrefixSlashToPath(fromPath);
        String tmpToPath = addPrefixSlashToPath(toPath);
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            pnfs.renameEntry(tmpFromPath, tmpToPath, false);
            result = pnfs.getFileAttributes(tmpToPath, REQUIRED_ATTRIBUTES);
        } catch (CacheException ex) {
            _log.warn("File '{}' could not get renamed to '{}', {}", fromPath, toPath, ex.getMessage());
        }
        return result;
    }

    /**
     * <p>
     * Creates a Directory in a specific file path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private FileAttributes renameFile(Subject subject, PnfsId pnfsid, String toPath)
    {
        FileAttributes result = null;
        String tmpToPath = addPrefixSlashToPath(toPath);
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            pnfs.renameEntry(pnfsid, tmpToPath, false);
            result = pnfs.getFileAttributes(tmpToPath, REQUIRED_ATTRIBUTES);
        } catch (CacheException ex) {
            _log.warn("File with PnfsId '{}' could not get renamed to '{}', {}", pnfsid, toPath, ex.getMessage());
        }
        return result;
    }

    /**
     * <p>
     * Deletes a File in a specific file path. Needed by function deleteRecursively.
     * </p>
     *
     * @param filePath
     *            {@link String} identifying a directory path
     */
    private boolean deleteFile(Subject subject, PnfsId pnfsid, String filePath)
    {
        boolean result = false;
        String tmpFilePath = addPrefixSlashToPath(filePath);
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            pnfs.deletePnfsEntry(pnfsid, tmpFilePath,
                    EnumSet.of(REGULAR, LINK));
            sendRemoveInfoToBilling(new FsPath(tmpFilePath));
            result = true;
        } catch (CacheException ex) {
            _log.warn("File '{}' could not get deleted, {}", filePath, ex.getMessage());
        }
        return result;
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
            billingStub.notify(infoRemove);
        } catch (NoRouteToCellException e) {
            _log.error("Cannot send remove message to billing: {}",
                       e.getMessage());
        }
    }

    /**
     * Creates a new file. The door will relay all data to the pool.
     * @param subject
     * @param path
     * @param inputStream
     * @param length
     * @return
     * @throws diskCacheV111.util.CacheException
     * @throws java.lang.InterruptedException
     * @throws java.io.IOException
     * @throws java.net.URISyntaxException
     */
    private boolean writeFile(Subject subject, FsPath path, InputStream inputStream, Long length)
            throws CacheException, InterruptedException, IOException,
                   URISyntaxException
    {
        boolean success = false;
        WriteTransfer transfer = new WriteTransfer(subject, pnfsHandler, path);
        transfers.put((int) transfer.getSessionId(), transfer);
        try {
            transfer.setProxyTransfer(true);
            transfer.createNameSpaceEntry();
            try {
                transfer.setLength(length);
                try {
                    transfer.selectPoolAndStartMover(ioQueue, retryPolicy);
                    String uri = transfer.waitForRedirect(moverTimeout, moverTimeoutUnit);
                    if (uri == null) {
                        throw new TimeoutCacheException("Server is busy (internal timeout)");
                    }
                    transfer.relayData(inputStream);
                } finally {
                    transfer.killMover(killTimeout, killTimeoutUnit);
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
            transfers.remove((int) transfer.getSessionId());
        }
        return success;
    }

    private String getWriteUrl(Subject subject, FsPath path, Long length)
            throws CacheException, InterruptedException,
                   URISyntaxException
    {
        String uri = null;
        WriteTransfer transfer = new WriteTransfer(subject, pnfsHandler, path);
        transfers.put((int) transfer.getSessionId(), transfer);
        try {
            transfer.createNameSpaceEntry();
            try {
                transfer.setLength(length);
                transfer.selectPoolAndStartMover(ioQueue, retryPolicy);
                uri = transfer.waitForRedirect(moverTimeout, moverTimeoutUnit);
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
                transfers.remove((int) transfer.getSessionId());
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
    private byte[] readFile(Subject subject, FsPath path, PnfsId pnfsid)
            throws CacheException, InterruptedException, IOException, URISyntaxException
    {
        byte[] result = null;
        ReadTransfer transfer = null;
        try {
            transfer = beginRead(subject, path, pnfsid, true);
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
            if (transfer != null) transfers.remove((int) transfer.getSessionId());
        }
        return result;
    }

    /**
     * Returns a read URL for a file.
     *
     * @param subject
     * @param path The full path of the file.
     * @param pnfsid The PNFS ID of the file.
     * @return
     * @throws diskCacheV111.util.CacheException
     * @throws java.lang.InterruptedException
     * @throws java.net.URISyntaxException
     */
    private String getReadUrl(Subject subject, FsPath path, PnfsId pnfsid)
            throws CacheException, InterruptedException, URISyntaxException
    {
        return beginRead(subject, path, pnfsid, false).getRedirect();
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
    private ReadTransfer beginRead(Subject subject, FsPath path, PnfsId pnfsid, boolean isProxyTransfer)
            throws CacheException, InterruptedException, URISyntaxException
    {
        String uri = null;
        ReadTransfer transfer = new ReadTransfer(pnfsHandler, subject, path, pnfsid);
        transfers.put((int) transfer.getSessionId(), transfer);
        try {
            transfer.setProxyTransfer(isProxyTransfer);
            transfer.readNameSpaceEntry();
            try {
                transfer.selectPoolAndStartMover(ioQueue, retryPolicy);
                uri = transfer.waitForRedirect(moverTimeout, moverTimeoutUnit);
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
                transfers.remove((int) transfer.getSessionId());
            }
        }
        return transfer;
    }

    private void initializeTransfer(Subject subject, HttpTransfer transfer)
            throws URISyntaxException
    {
        transfer.setLocation(getLocation());
        transfer.setCellName(getCellName());
        transfer.setDomainName(getCellDomainName());
        transfer.setPoolManagerStub(poolMgrStub);
        transfer.setPoolStub(poolStub);
        transfer.setBillingStub(billingStub);
        transfer.setClientAddress(new InetSocketAddress(Subjects
                .getOrigin(subject).getAddress(),
                PROTOCOL_INFO_UNKNOWN_PORT));
        transfer.setOverwriteAllowed(isOverwriteAllowed);
    }

    /**
     * Specialisation of the Transfer class for HTTP transfers.
     */
    private class HttpTransfer extends RedirectedTransfer<String>
    {
        private URI _location;
        private InetSocketAddress _clientAddressForPool;

        public HttpTransfer(Subject subject, PnfsHandler pnfs, FsPath path)
                throws URISyntaxException
        {
            super(pnfs, subject, path);
            initializeTransfer(subject, this);
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
                _clientAddressForPool = new InetSocketAddress(internalAddress, 0);
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
            super(subject, pnfs, path);
            setPnfsId(pnfsid);
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

                if (!waitForMover(transferConfirmationTimeout, transferConfirmationTimeoutUnit)) {
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

            transfers.remove((int) getSessionId());

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
        public WriteTransfer(Subject subject, PnfsHandler pnfs, FsPath path)
                throws URISyntaxException
        {
            super(subject, pnfs, path);
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

                if (!waitForMover(transferConfirmationTimeout, transferConfirmationTimeoutUnit)) {
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
            transfers.remove((int) getSessionId());
            if (error == null) {
                notifyBilling(0, "");
            } else {
                notifyBilling(error.getRc(), error.getMessage());
            }
        }
    }
}
