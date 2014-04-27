/*
 * Copyright (c) 2010, Sun Microsystems, Inc.
 * Copyright (c) 2010, The Storage Networking Industry Association.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 *
 * Neither the name of The Storage Networking Industry Association (SNIA) nor
 * the names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 *
 *  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 *  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *  THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.dcache.cdmi.dao.impl;

import static org.dcache.namespace.FileAttribute.*;
import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
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
import javax.servlet.ServletContext;
import org.dcache.auth.Subjects;
import org.dcache.cdmi.mover.DCacheDataTransfer;
import org.dcache.cdmi.mover.CDMIProtocolInfo;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellLifeCycleAware;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.ACLException;
import org.dcache.cdmi.model.DCacheDataObject;
import org.dcache.cdmi.tool.IDConverter;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;
import org.dcache.util.list.DirectoryEntry;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.ListDirectoryHandler;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.dao.ContainerDao;
import org.snia.cdmiserver.dao.DataObjectDao;
import org.snia.cdmiserver.exception.BadRequestException;
import org.snia.cdmiserver.exception.ConflictException;
import org.snia.cdmiserver.model.DataObject;
import org.springframework.web.context.ServletContextAware;

/**
 * <p>
 * Concrete implementation of {@link DataObjectDao} using the local filesystem as the backing store.
 * </p>
 */
public class DCacheDataObjectDaoImpl extends AbstractCellComponent
    implements DataObjectDao, CellLifeCycleAware//, ServletContextAware
{

    //
    // Something important
    //
    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DCacheDataObjectDaoImpl.class);

    // -------------------------------------------------------------- Properties
    private String baseDirectoryName = null;

    //
    // Properties and Dependency Injection Methods by dCache
    //
    private ServletContext servletContext = null;
    private CellStub pnfsStub;
    private PnfsHandler pnfsHandler;
    private ListDirectoryHandler listDirectoryHandler;
    private CellStub poolStub;
    private CellStub poolMgrStub;
    private CellStub billingStub;
    private static final String ATTRIBUTE_NAME_PNFSSTUB = "org.dcache.cdmi.pnfsstub";
    private static final String ATTRIBUTE_NAME_LISTER = "org.dcache.cdmi.lister";
    private static final String ATTRIBUTE_NAME_POOLSTUB = "org.dcache.cdmi.poolstub";
    private static final String ATTRIBUTE_NAME_POOLMGRSTUB = "org.dcache.cdmi.poolmgrstub";
    private static final String ATTRIBUTE_NAME_BILLINGSTUB = "org.dcache.cdmi.billingstub";
    private PnfsId pnfsId;
    private long accessTime;
    private long creationTime;
    private long changeTime;
    private long modificationTime;
    private long size;
    private int owner;
    private ACL acl;
    private FileType fileType;

    //
    public void setBaseDirectoryName(String baseDirectoryName)
    {
        this.baseDirectoryName = baseDirectoryName;
        _log.debug("******* Base Directory (O) = " + baseDirectoryName);
        //Temp Helper Part
        if (this.baseDirectoryName != null) DCacheDataTransfer.setBaseDirectoryName2(this.baseDirectoryName);
    }

    public void setPnfsStub(CellStub pnfsStub)
    {
        this.pnfsStub = pnfsStub;
        if (this.pnfsStub == null) _log.error("DCacheDataObjectDaoImpl: PnfsStub is null!");
        _log.debug("******* PnfsStub (O) = " + pnfsStub.toString());
        this.pnfsHandler = new PnfsHandler(this.pnfsStub);
    }

    public void setListDirectoryHandler(ListDirectoryHandler listDirectoryHandler)
    {
        this.listDirectoryHandler = listDirectoryHandler;
        if (this.listDirectoryHandler == null) _log.error("DCacheDataObjectDaoImpl: ListDirectoryHandler is null!");
        _log.debug("******* ListDirectoryHandler (O) = " + listDirectoryHandler.toString());
    }

    public void setPoolStub(CellStub poolStub)
    {
        this.poolStub = poolStub;
        if (this.poolStub == null) _log.error("DCacheDataObjectDaoImpl: PoolStub is null!");
        _log.debug("******* PoolStub (O) = " + poolStub.toString());
    }

    public void setPoolMgrStub(CellStub poolMgrStub)
    {
        this.poolMgrStub = poolMgrStub;
        if (this.poolMgrStub == null) _log.error("DCacheDataObjectDaoImpl: PoolMgrStub is null!");
        _log.debug("******* PoolMgrStub (O) = " + poolMgrStub.toString());
    }

    public void setBillingStub(CellStub billingStub)
    {
        this.billingStub = billingStub;
        if (this.billingStub == null) _log.error("DCacheDataObjectDaoImpl: BillingStub is null!");
        _log.debug("******* BillingStub (O) = " + billingStub.toString());
    }

    /**
     * <p>
     * Injected {@link ContainerDao} instance.
     * </p>
     */
    private ContainerDao containerDao;

    public void setContainerDao(ContainerDao containerDao)
    {
        this.containerDao = containerDao;
    }

    public DCacheDataObjectDaoImpl()
    {
        _log.debug("Re-Init DCacheDataObjectDaoImpl...");
        if (listDirectoryHandler == null) {
            init();
        }
    }

    // ---------------------------------------------------- ContainerDao Methods
    // utility function
    // given a path, find out metadata file name and container directory
    String getmetadataFileName(String path)
    {
        // Make sure we have a file name for the object
        // check for file name
        // path should be <container name>/<file name>
        // Split path into path and filename
        String[] tokens = path.split("/");
        if (tokens.length < 1) {
            throw new BadRequestException("No object name in path <" + path + ">");
        }

        String fileName = tokens[tokens.length - 1];

        return "." + fileName;
    }

    //
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
        String fileName = tokens[tokens.length - 1];
        String containerName = "";
        for (int i = 0; i <= tokens.length - 2; i++) {
            containerName += tokens[i] + "/";
        }
        return containerName;
    }

    @Override
    public DCacheDataObject createByPath(String path, DataObject dObj) throws Exception {
        //
        String containerName = getcontainerName(path);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        DCacheDataObject newDObj = (DCacheDataObject) dObj;

        //
        File objFile, baseDirectory, containerDirectory;
        try {
            _log.debug("baseDirectory = " + baseDirectoryName);
            baseDirectory = new File(baseDirectoryName + "/");
            _log.debug("Base Directory Absolute Path = " + baseDirectory.getAbsolutePath());
            containerDirectory = new File(baseDirectory, containerName);
            _log.debug("Container Absolute Path = " + containerDirectory.getAbsolutePath());
            //
            objFile = new File(baseDirectory, path);
            _log.debug("Object Absolute Path = " + objFile.getAbsolutePath());
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.error("Exception while writing: " + ex);
            throw new IllegalArgumentException("Cannot write Object @" + path + " error : " + ex);
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
            if (!writeFile(objFile.getAbsolutePath(), newDObj.getValue())) {
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
                    _log.debug("DCacheDataObjectDao<Create>, setPnfsID: " + pnfsId.toIdString());
                    newDObj.setPnfsID(pnfsId.toIdString());
                    long ctime = (attr.getCreationTime() > creationTime) ? attr.getCreationTime() : creationTime;
                    long atime = (attr.getAccessTime() > accessTime) ? attr.getAccessTime() : accessTime;
                    long mtime = (attr.getModificationTime() > modificationTime) ? attr.getModificationTime() : modificationTime;
                    long osize = (attr.getSize() > size) ? attr.getSize() : size;
                    newDObj.setMetadata("cdmi_ctime", sdf.format(ctime));
                    newDObj.setMetadata("cdmi_atime", sdf.format(atime));
                    newDObj.setMetadata("cdmi_mtime", sdf.format(mtime));
                    newDObj.setMetadata("cdmi_size", String.valueOf(osize));
                    oowner = attr.getOwner();
                    oacl = attr.getAcl();
                    objectID = new IDConverter().toObjectID(pnfsId.toIdString());
                    newDObj.setObjectID(objectID);
                    _log.debug("DCacheDataObjectDao<Create>, setObjectID: " + objectID);
                } else {
                    _log.error("DCacheDataObjectDao<Create>, Cannot read PnfsId from meta information, ObjectID will be empty");
                }
            } else {
                _log.error("DCacheDataObjectDao<Create>, Cannot read meta information from directory: " + objFile.getAbsolutePath());
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
            ex.printStackTrace();
            _log.error("Exception while writing: " + ex);
            throw new IllegalArgumentException("Cannot write Object @" + path + " error : " + ex);
        }
        return newDObj;
    }

    @Override
    public void deleteByPath(String path)
    {
        throw new UnsupportedOperationException("DCacheDataObjectDaoImpl.deleteByPath()");
    }

    @Override
    public DCacheDataObject findByPath(String path)
    {

        // ISO-8601 Date
        Date now = new Date();
        long nowAsLong = now.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        _log.debug("In DCacheDataObjectDao.findByPath : " + path);
        //
        String containerName = getcontainerName(path);
        //
        // Check for metadata file
        File objFile, baseDirectory;
        try {
            _log.debug("baseDirectory = " + baseDirectoryName);
            baseDirectory = new File(baseDirectoryName + "/" + containerName);
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.error("Exception in findByPath : " + ex);
            throw new IllegalArgumentException("Cannot get Object @" + path + " error : " + ex);
        }

        //temp
        if (listDirectoryHandler == null) {
            init();
        }

        // Check for object file
        try {
            _log.debug("baseDirectory = " + baseDirectoryName);
            baseDirectory = new File(baseDirectoryName + "/");
            objFile = new File(baseDirectory, path);
            _log.debug("Object Absolute Path = " + objFile.getAbsolutePath());
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.error("Exception in findByPath : " + ex);
            throw new IllegalArgumentException("Cannot get Object @" + path + " error : " + ex);
        }

        if (!checkIfDirectoryFileExists(objFile.getAbsolutePath())) {
            return null;
        }
        //
        // Both Files are there. So open, read, create object and send out
        //
        DCacheDataObject dObj = new DCacheDataObject();
        try {
            // Read object from file
            byte[] inBytes = readFile(objFile.getAbsolutePath());
            dObj.setValue(new String(inBytes));

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
                    _log.debug("DCacheDataObjectDao<Read>, setPnfsID: " + pnfsId.toIdString());
                    dObj.setPnfsID(pnfsId.toIdString());
                    long ctime = (attr.getCreationTime() > creationTime) ? attr.getCreationTime() : creationTime;
                    long atime = (attr.getAccessTime() > accessTime) ? attr.getAccessTime() : accessTime;
                    long mtime = (attr.getModificationTime() > modificationTime) ? attr.getModificationTime() : modificationTime;
                    long osize = (attr.getSize() > size) ? attr.getSize() : size;
                    dObj.setMetadata("cdmi_ctime", sdf.format(ctime));
                    dObj.setMetadata("cdmi_atime", sdf.format(atime));
                    dObj.setMetadata("cdmi_mtime", sdf.format(mtime));
                    dObj.setMetadata("cdmi_size", String.valueOf(osize));
                    oowner = attr.getOwner();
                    oacl = attr.getAcl();
                    objectID = new IDConverter().toObjectID(pnfsId.toIdString());
                    dObj.setObjectID(objectID);
                    _log.debug("DCacheDataObjectDao<Read>, setObjectID: " + objectID);
                } else {
                    _log.error("DCacheDataObjectDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
                }
            } else {
                _log.error("DCacheDataObjectDao<Read>, Cannot read meta information from object: " + objFile.getAbsolutePath());
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
            ex.printStackTrace();
            _log.error("Exception while reading: " + ex);
            throw new IllegalArgumentException("Cannot read Object @" + path + " error : " + ex);
        }

        // change access time
        try {
            FileAttributes attr = new FileAttributes();
            attr.setAccessTime(nowAsLong);
            pnfsHandler.setFileAttributes(pnfsId, attr);
            dObj.setMetadata("cdmi_atime", sdf.format(now));
        } catch (CacheException ex) {
            _log.error("DCacheDataObjectDao<Read>, Cannot update meta information for object with objectID " + dObj.getObjectID());
        }

        return dObj;
    }

    @Override
    public DCacheDataObject findByObjectId(String objectId)
    {
        throw new UnsupportedOperationException("DCacheDataObjectDaoImpl.findByObjectId()");
    }
    // --------------------------------------------------------- Private Methods

    /**
     * DCache related stuff.
     */

    // Temp Helper Function
    private void init()
    {
        pnfsStub = DCacheDataTransfer.getPnfsStub2();
        pnfsHandler = DCacheDataTransfer.getPnfsHandler2();
        listDirectoryHandler = DCacheDataTransfer.getListDirectoryHandler2();
        poolStub = DCacheDataTransfer.getPoolStub2();
        poolMgrStub = DCacheDataTransfer.getPoolMgrStub2();
        billingStub = DCacheDataTransfer.getBillingStub2();
        baseDirectoryName = DCacheDataTransfer.getBaseDirectoryName2();
    }

    @Override
    public void afterStart()
    {
        _log.debug("Start DCacheDataObjectDaoImpl...");
    }

    @Override
    public void beforeStop()
    {
    }

    private CellStub getCellStubAttribute()
    {
        if (servletContext == null) {
            throw new RuntimeException("ServletContext is not set");
        }
        Object attribute = servletContext.getAttribute(ATTRIBUTE_NAME_PNFSSTUB);
        if (attribute == null) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_PNFSSTUB + " not found");
        }
        if (!CellStub.class.isInstance(attribute)) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_PNFSSTUB + " not of type " + CellStub.class);
        }
        return (CellStub) attribute;
    }

    private ListDirectoryHandler getListDirAttribute()
    {
        if (servletContext == null) {
            throw new RuntimeException("ServletContext is not set");
        }
        Object attribute = servletContext.getAttribute(ATTRIBUTE_NAME_LISTER);
        if (attribute == null) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_LISTER + " not found");
        }
        if (!ListDirectoryHandler.class.isInstance(attribute)) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_LISTER + " not of type " + ListDirectoryHandler.class);
        }
        return (ListDirectoryHandler) attribute;
    }

    private CellStub getPoolAttribute()
    {
        if (servletContext == null) {
            throw new RuntimeException("ServletContext is not set");
        }
        Object attribute = servletContext.getAttribute(ATTRIBUTE_NAME_POOLSTUB);
        if (attribute == null) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_POOLSTUB + " not found");
        }
        if (!CellStub.class.isInstance(attribute)) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_POOLSTUB + " not of type " + CellStub.class);
        }
        return (CellStub) attribute;
    }

    private CellStub getPoolMgrAttribute()
    {
        if (servletContext == null) {
            throw new RuntimeException("ServletContext is not set");
        }
        Object attribute = servletContext.getAttribute(ATTRIBUTE_NAME_POOLMGRSTUB);
        if (attribute == null) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_POOLMGRSTUB + " not found");
        }
        if (!CellStub.class.isInstance(attribute)) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_POOLMGRSTUB + " not of type " + CellStub.class);
        }
        return (CellStub) attribute;
    }

    private CellStub getBillingAttribute()
    {
        if (servletContext == null) {
            throw new RuntimeException("ServletContext is not set");
        }
        Object attribute = servletContext.getAttribute(ATTRIBUTE_NAME_BILLINGSTUB);
        if (attribute == null) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_BILLINGSTUB + " not found");
        }
        if (!CellStub.class.isInstance(attribute)) {
            throw new RuntimeException("Attribute " + ATTRIBUTE_NAME_BILLINGSTUB + " not of type " + CellStub.class);
        }
        return (CellStub) attribute;
    }

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

    private Map<String, FileAttributes> listDirectoriesFilesByPath(String path)
    {
        String tmpPath = addPrefixSlashToPath(path);
        FsPath fsPath = new FsPath(tmpPath);
        Map<String, FileAttributes> result = new HashMap<>();
        try {
            listDirectoryHandler.printDirectory(Subjects.ROOT, new ListPrinter(result), fsPath, null, Range.<Integer>all());
        } catch (InterruptedException | CacheException ex) {
            _log.warn("DCacheDataObjectDaoImpl, Directory and file listing for path '" + path + "' was not possible, internal error message: " + ex.getMessage());
        }
        return result;
    }

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

    private String removeSlashesFromPath(String path)
    {
        String result = "";
        if (path != null && path.length() > 0) {
            if (path.startsWith("/")) {
                result = path.substring(1, path.length() - 1);
            } else {
                result = path;
            }
            if (result.endsWith("/")) {
                result = result.substring(0, result.length() - 2);
            } else {
                result = path;
            }
        }
        return result;
    }

    @Override
    public DCacheDataObject createById(String string, DataObject d)
    {
        throw new UnsupportedOperationException("DCacheDataObjectDaoImpl, Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    //@Override
    public void setServletContext(ServletContext sContext) {
        _log.debug("Init DCacheDataObjectDaoImpl...");
        this.servletContext = sContext;
        this.pnfsStub = getCellStubAttribute();
        this.pnfsHandler = new PnfsHandler(pnfsStub);
        //this.listDirectoryHandler = new ListDirectoryHandler(pnfsHandler); //does not work, tested 100 times
        this.listDirectoryHandler = getListDirAttribute(); //it only works in this way, tested 100 times
        this.poolStub = getPoolAttribute();
        this.poolMgrStub = getPoolMgrAttribute();
        this.billingStub = getBillingAttribute();
        //Temp Helper Part
        if (baseDirectoryName != null) DCacheDataTransfer.setBaseDirectoryName2(baseDirectoryName);
        DCacheDataTransfer.setPnfsStub2(pnfsStub);
        DCacheDataTransfer.setPnfsHandler2(pnfsHandler);
        DCacheDataTransfer.setListDirectoryHandler2(listDirectoryHandler);
        DCacheDataTransfer.setPoolStub2(poolStub);
        DCacheDataTransfer.setPoolMgrStub2(poolMgrStub);
        DCacheDataTransfer.setBillingStub2(billingStub);
    }

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

    public boolean writeFile(String filePath, String data)
    {
        boolean result = false;
        try {
            //The order of all commands is very important!
            Subject subject = Subjects.ROOT;
            DCacheDataTransfer.setData(data);
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
                }
                finally {
                    pnfsId = DCacheDataTransfer.getPnfsId();
                    creationTime = DCacheDataTransfer.getCreationTime();
                    accessTime = DCacheDataTransfer.getAccessTime();
                    changeTime = DCacheDataTransfer.getChangeTime();
                    modificationTime = DCacheDataTransfer.getModificationTime();
                    size = DCacheDataTransfer.getSize();
                    owner = DCacheDataTransfer.getOwner();
                    acl = DCacheDataTransfer.getACL();
                    fileType = DCacheDataTransfer.getFileType();
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                _log.debug("DCacheDataObjectDaoImpl<Write>-isWrite:" + transfer.isWrite());
                if (pnfsId != null) _log.debug("DCacheDataObjectDaoImpl<Write>-pnfsId:" + pnfsId);
                _log.debug("DCacheDataObjectDaoImpl<Write>-creationTime:" + sdf.format(creationTime));
                _log.debug("DCacheDataObjectDaoImpl<Write>-accessTime:" + sdf.format(accessTime));
                _log.debug("DCacheDataObjectDaoImpl<Write>-changeTime:" + sdf.format(changeTime));
                _log.debug("DCacheDataObjectDaoImpl<Write>-modificationTime:" + sdf.format(modificationTime));
                _log.debug("DCacheDataObjectDaoImpl<Write>-size:" + size);
                _log.debug("DCacheDataObjectDaoImpl<Write>-owner:" + owner);
                if (acl != null) _log.debug("DCacheDataObjectDaoImpl<Write>-acl:" + acl.toString());
                if (acl != null) _log.debug("DCacheDataObjectDaoImpl<Write>-aclExtraFormat:" + acl.toExtraFormat());
                if (acl != null) _log.debug("DCacheDataObjectDaoImpl<Write>-aclNFSv4String:" + acl.toNFSv4String());
                if (acl != null) _log.debug("DCacheDataObjectDaoImpl<Write>-aclOrgString:" + acl.toOrgString());
                if (fileType != null) _log.debug("DCacheDataObjectDaoImpl<Write>-fileType:" + fileType.toString());
                _log.debug("DCacheDataObjectDaoImpl<Write>-data:" + data);
                result = true;
            } finally {
            }
        } catch (CacheException | InterruptedException | UnknownHostException | ACLException ex) {
            _log.error("DCacheDataObjectDaoImpl, File could not become written, exception is: " + ex.getMessage());
        }
        return result;
    }

    public byte[] readFile(String filePath)
    {
        byte[] result = null;
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
                    result = DCacheDataTransfer.getDataAsBytes();
                    _log.debug("DCacheDataObjectDaoImpl received data: " + result.toString());
                }
                finally {
                    pnfsId = DCacheDataTransfer.getPnfsId();
                    creationTime = DCacheDataTransfer.getCreationTime();
                    accessTime = DCacheDataTransfer.getAccessTime();
                    changeTime = DCacheDataTransfer.getChangeTime();
                    modificationTime = DCacheDataTransfer.getModificationTime();
                    size = DCacheDataTransfer.getSize();
                    owner = DCacheDataTransfer.getOwner();
                    acl = DCacheDataTransfer.getACL();
                    fileType = DCacheDataTransfer.getFileType();
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                _log.debug("DCacheDataObjectDaoImpl<Read>-isWrite:" + transfer.isWrite());
                if (pnfsId != null) _log.debug("DCacheDataObjectDaoImpl<Read>-pnfsId:" + pnfsId);
                _log.debug("DCacheDataObjectDaoImpl<Read>-creationTime:" + sdf.format(creationTime));
                _log.debug("DCacheDataObjectDaoImpl<Read>-accessTime:" + sdf.format(accessTime));
                _log.debug("DCacheDataObjectDaoImpl<Read>-changeTime:" + sdf.format(changeTime));
                _log.debug("DCacheDataObjectDaoImpl<Read>-modificationTime:" + sdf.format(modificationTime));
                _log.debug("DCacheDataObjectDaoImpl<Read>-size:" + size);
                _log.debug("DCacheDataObjectDaoImpl<Read>-owner:" + owner);
                if (acl != null) _log.debug("DCacheDataObjectDaoImpl<Read>-acl:" + acl.toString());
                if (acl != null) _log.debug("DCacheDataObjectDaoImpl<Read>-aclExtraFormat:" + acl.toExtraFormat());
                if (acl != null) _log.debug("DCacheDataObjectDaoImpl<Read>-aclNFSv4String:" + acl.toNFSv4String());
                if (acl != null) _log.debug("DCacheDataObjectDaoImpl<Read>-aclOrgString:" + acl.toOrgString());
                if (fileType != null) _log.debug("DCacheDataObjectDaoImpl<Read>-fileType:" + fileType.toString());
                _log.debug("DCacheDataObjectDaoImpl<Read>-data:" + result.toString());
            } finally {
            }
        } catch (CacheException | InterruptedException | UnknownHostException | ACLException ex) {
            _log.error("DCacheDataObjectDaoImpl, File could not become read, exception is: " + ex.getMessage());
        }
        return result;
    }

    public static class HelperClass
    {
        public static void sleep(long ms)
        {
            try {
                Thread.sleep(ms);
            } catch (InterruptedException ex) {
                Logger.getLogger(DCacheDataObjectDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }

}
