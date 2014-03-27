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

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.security.auth.Subject;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import org.dcache.auth.Subjects;
import org.dcache.cdmi.mover.CDMIDataTransfer;
import dmg.cells.nucleus.CellLifeCycleAware;
import org.dcache.cdmi.mover.CDMIProtocolInfo;
import dmg.cells.nucleus.AbstractCellComponent;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.*;
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
import org.snia.cdmiserver.exception.BadRequestException;
import org.snia.cdmiserver.exception.NotFoundException;
import org.snia.cdmiserver.model.Container;
import diskCacheV111.util.PnfsId;
import java.text.ParseException;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.ACLException;
import org.dcache.cdmi.dao.CDMIContainerDao;
import org.dcache.cdmi.model.CDMIContainer;
import org.dcache.cdmi.tool.IDConverter;

/**
 * <p>
 * Concrete implementation of {@link ContainerDao} using the local filesystem as the backing store.
 * </p>
 */
public class CDMIContainerDaoImpl extends AbstractCellComponent
    implements CDMIContainerDao, ServletContextListener, CellLifeCycleAware {

    //
    // Something important
    //
    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(CDMIContainerDaoImpl.class);

    //
    // Properties and Dependency Injection Methods by CDMI
    //
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

    /**
     * <p>
     * Set the base directory name for our local storage.
     * </p>
     *
     * @param baseDirectory
     *            The new base directory name
     */
    public void setBaseDirectoryName(String baseDirectoryName) {
        this.baseDirectoryName = baseDirectoryName;
        _log.debug("******* Base Directory (C) = " + baseDirectoryName);
        //Temp Helper Part
        if (this.baseDirectoryName != null) CDMIDataTransfer.setBaseDirectoryName(this.baseDirectoryName);
    }

    private boolean recreate = true;

    public CDMIContainerDaoImpl() {
        _log.debug("Re-Init CDMIContainerDaoImpl...");
        if (listDirectoryHandler == null) {
            init();
        }
    }

    /**
     * <p>
     * Set the "recreate on first use" flag that (if set) will cause any previous contents of the
     * base directory to be erased on first access. Default value for this flag is
     * <code>false</code>.
     * </p>
     *
     * @param recreate
     *            The new recreate flag value
     */
    public void setRecreate(boolean recreate) {
        this.recreate = recreate;
    }

    //
    // ContainerDao Methods invoked from PathResource
    //
    @Override
    public CDMIContainer createByPath(String path, CDMIContainer containerRequest) {

        //
        // The User metadata and exports have already been de-serialized into the
        // passed Container in PathResource.putContainer()
        //

        File directory = absoluteFile(path);

        _log.debug("Create container <path>: " + directory.getAbsolutePath());

        //File containerFieldsFile = getContainerFieldsFile(path);

        //
        // Setup ISO-8601 Date
        //
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        if (containerRequest.getMove() == null) { // This is a normal Create or Update

            //
            // Underlying Directory existence determines whether this is a Create or
            // Update.
            //

            if (!checkIfDirectoryFileExists(directory.getAbsolutePath())) { // Creating Container

                _log.debug("<Container Create>");

                if (!createDirectory(directory.getAbsolutePath())) {
                    throw new IllegalArgumentException("Cannot create container '" + path + "'");
                }

                // OLD:
                // String objectID = ObjectID.getObjectID(9); // System.nanoTime()+"";
                String objectID = "";
                int cowner = 0;
                ACL cacl = null;
                FileAttributes attr = getAttributesByPath(directory.getAbsolutePath());
                if (attr != null) {
                    pnfsId = attr.getPnfsId();
                    if (pnfsId != null) {
                        // update with real info
                        _log.debug("CDMIContainerDao<Create>, setPnfsID: " + pnfsId.toIdString());
                        containerRequest.setPnfsID(pnfsId.toIdString());
                        containerRequest.setMetadata("cdmi_ctime", sdf.format(attr.getCreationTime()));
                        containerRequest.setMetadata("cdmi_atime", sdf.format(attr.getAccessTime()));
                        containerRequest.setMetadata("cdmi_mtime", sdf.format(attr.getModificationTime()));
                        containerRequest.setMetadata("cdmi_size", String.valueOf(attr.getSize()));
                        cowner = attr.getOwner();
                        cacl = attr.getAcl();
                        objectID = new IDConverter().toObjectID(pnfsId.toIdString());
                        containerRequest.setObjectID(objectID);
                        _log.debug("CDMIContainerDao<Create>, setObjectID: " + objectID);
                    } else {
                        _log.error("CDMIContainerDao<Create>, Cannot read PnfsId from meta information, ObjectID will be empty");
                    }
                } else {
                    _log.error("CDMIContainerDao<Create>, Cannot read meta information from directory: " + directory.getAbsolutePath());
                }

                //
                // TODO: Use Parent capabiltiesURI if not specified in create body
                //

                containerRequest.setCapabilitiesURI("/cdmi_capabilities/container/default");

                //
                // TODO: Use Parent Domain if not specified in create body
                //
                if (containerRequest.getDomainURI() == null) {
                    containerRequest.setDomainURI("/cdmi_domains/default_domain");
                }

                // NOT SUPPORTED YET:
                Map<String, Object> exports = containerRequest.getExports();
                if (exports.containsKey("OCCI/NFS")) {
                    // Export this directory (OpenSolaris only so far)
                    // Runtime runtime = Runtime.getRuntime();
                    // String exported =
                    // "pfexec share -f nfs -o rw=10.1.254.117:10.1.254.122:10.1.254.123:10.1.254.124:10.1.254.125:10.1.254.126:10.1.254.127 "
                    // + containerFieldsFile.getAbsolutePath();
                    // runtime.exec(exported);
                }

                containerRequest.setMetadata("cdmi_ctime", sdf.format(now));
                containerRequest.setMetadata("cdmi_acount", "0");
                containerRequest.setMetadata("cdmi_mcount", "0");
                containerRequest.setMetadata("cdmi_owner", Subjects.ROOT.toString());
                containerRequest.setMetadata("cdmi_owner", String.valueOf(cowner));  //TODO
                if (cacl != null && !cacl.isEmpty()) {
                    ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                    for (ACE ace : cacl.getList()) {
                        HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                        subMetadataEntry_ACL.put("acetype", ace.getType().name());
                        subMetadataEntry_ACL.put("identifier", ace.getWho().name());
                        subMetadataEntry_ACL.put("aceflags", String.valueOf(ace.getFlags()));
                        subMetadataEntry_ACL.put("acemask", String.valueOf(ace.getAccessMsk()));
                        subMetadata_ACL.add(subMetadataEntry_ACL);
                    }
                    containerRequest.setSubMetadata_ACL(subMetadata_ACL);
                } else {
                    ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                    subMetadataEntry_ACL.put("acetype", "ALLOW");
                    subMetadataEntry_ACL.put("identifier", "OWNER@");
                    subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                    subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                    subMetadata_ACL.add(subMetadataEntry_ACL);
                    containerRequest.setSubMetadata_ACL(subMetadata_ACL);
                }

            } else { // Updating Container

                _log.debug("<Container Update>");

                //
                // Read the persistent metadata from the "." file
                //
                //TODO:
                //CDMIContainer currentContainer = getPersistedContainerFields(containerFieldsFile);
                CDMIContainer currentContainer = new CDMIContainer();

                String objectID = "";
                int cowner = 0;
                ACL cacl = null;
                FileAttributes attr = getAttributesByPath(directory.getAbsolutePath());
                if (attr != null) {
                    pnfsId = attr.getPnfsId();
                    if (pnfsId != null) {
                        // update with real info
                        _log.debug("CDMIContainerDao<Update>, setPnfsID: " + pnfsId.toIdString());
                        currentContainer.setPnfsID(pnfsId.toIdString());
                        currentContainer.setMetadata("cdmi_ctime", sdf.format(attr.getCreationTime()));
                        currentContainer.setMetadata("cdmi_atime", sdf.format(attr.getAccessTime()));
                        currentContainer.setMetadata("cdmi_mtime", sdf.format(attr.getModificationTime()));
                        currentContainer.setMetadata("cdmi_size", String.valueOf(attr.getSize()));
                        cowner = attr.getOwner();
                        cacl = attr.getAcl();
                        objectID = new IDConverter().toObjectID(pnfsId.toIdString());
                        currentContainer.setObjectID(objectID);
                        _log.debug("CDMIContainerDao<Update>, setObjectID: " + objectID);
                    } else {
                        _log.error("CDMIContainerDao<Update>, Cannot read PnfsId from meta information, ObjectID will be empty");
                    }
                } else {
                    _log.error("CDMIContainerDao<Update>, Cannot read meta information from directory: " + directory.getAbsolutePath());
                }

                currentContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");

                if (currentContainer.getDomainURI() == null) {
                    currentContainer.setDomainURI("/cdmi_domains/default_domain");
                }

                currentContainer.setMetadata("cdmi_acount", "0");
                currentContainer.setMetadata("cdmi_mcount", "0");
                currentContainer.setMetadata("cdmi_owner", String.valueOf(cowner));  //TODO
                if (cacl != null && !cacl.isEmpty()) {
                    ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                    for (ACE ace : cacl.getList()) {
                        HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                        subMetadataEntry_ACL.put("acetype", ace.getType().name());
                        subMetadataEntry_ACL.put("identifier", ace.getWho().name());
                        subMetadataEntry_ACL.put("aceflags", String.valueOf(ace.getFlags()));
                        subMetadataEntry_ACL.put("acemask", String.valueOf(ace.getAccessMsk()));
                        subMetadata_ACL.add(subMetadataEntry_ACL);
                    }
                    currentContainer.setSubMetadata_ACL(subMetadata_ACL);
                } else {
                    ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                    subMetadataEntry_ACL.put("acetype", "ALLOW");
                    subMetadataEntry_ACL.put("identifier", "OWNER@");
                    subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                    subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                    subMetadata_ACL.add(subMetadataEntry_ACL);
                    currentContainer.setSubMetadata_ACL(subMetadata_ACL);
                }

                containerRequest.setPnfsID(pnfsId.toIdString());
                containerRequest.setObjectID(objectID);

                //
                // TODO: Need to handle update of Capabilities URI
                //
                containerRequest.setCapabilitiesURI(currentContainer.getCapabilitiesURI());

                //
                // TODO: Need to handle update of Domain
                //
                containerRequest.setDomainURI(currentContainer.getDomainURI());

                // NOT SUPPORTED YET:
                Map<String, Object> exports = containerRequest.getExports();
                if (exports.containsKey("OCCI/NFS")) {
                    if (currentContainer.getExports().containsKey("OCCI/NFS")) {
                        // Do nothing - already exported
                    } else {
                        // Export this directory (OpenSolaris only so far)
                        // Runtime runtime = Runtime.getRuntime();
                        // String exported =
                        // "pfexec share -f nfs -o rw=10.1.254.117:10.1.254.122:10.1.254.123:10.1.254.124:10.1.254.125:10.1.254.126:10.1.254.127"
                        // + containerFieldsFile.getAbsolutePath();
                        // runtime.exec(exported);
                    }
                }

                //forth-and-back update
                for (String key : containerRequest.getMetadata().keySet()) {
                    currentContainer.setMetadata(key, containerRequest.getMetadata().get(key));
                }
                for (String key : currentContainer.getMetadata().keySet()) {
                    containerRequest.setMetadata(key, currentContainer.getMetadata().get(key));
                }
                containerRequest.setMetadata("cdmi_mtime", sdf.format(now));

                //
                // TODO: Need to handle update of ACL info
                //
                containerRequest.setSubMetadata_ACL(currentContainer.getSubMetadata_ACL());  //doesn't really work
                if (cacl != null && !cacl.isEmpty()) {
                    ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                    for (ACE ace : cacl.getList()) {
                        HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                        subMetadataEntry_ACL.put("acetype", ace.getType().name());
                        subMetadataEntry_ACL.put("identifier", ace.getWho().name());
                        subMetadataEntry_ACL.put("aceflags", String.valueOf(ace.getFlags()));
                        subMetadataEntry_ACL.put("acemask", String.valueOf(ace.getAccessMsk()));
                        subMetadata_ACL.add(subMetadataEntry_ACL);
                    }
                    containerRequest.setSubMetadata_ACL(subMetadata_ACL);
                } else {
                    ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                    subMetadataEntry_ACL.put("acetype", "ALLOW");
                    subMetadataEntry_ACL.put("identifier", "OWNER@");
                    subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                    subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                    subMetadata_ACL.add(subMetadataEntry_ACL);
                    containerRequest.setSubMetadata_ACL(subMetadata_ACL);
                }

            }

            //
            // Write created or updated persisted fields out to the "." file
            //

            // update meta information
            try {
                FileAttributes attr = new FileAttributes();
                Date ctime = sdf.parse(containerRequest.getMetadata().get("cdmi_ctime"));
                Date atime = sdf.parse(containerRequest.getMetadata().get("cdmi_atime"));
                Date mtime = sdf.parse(containerRequest.getMetadata().get("cdmi_mtime"));
                long ctimeAsLong = ctime.getTime();
                long atimeAsLong = atime.getTime();
                long mtimeAsLong = mtime.getTime();
                attr.setCreationTime(ctimeAsLong);
                attr.setAccessTime(atimeAsLong);
                attr.setModificationTime(mtimeAsLong);
                PnfsId id = new PnfsId(containerRequest.getPnfsID());
                pnfsHandler.setFileAttributes(id, attr);
            } catch (CacheException | ParseException ex) {
                _log.error("CDMIContainerDao<Update>, Cannot update meta information for object with objectID " + containerRequest.getObjectID());
            }

            //
            // Transient fields
            //
            containerRequest.setCompletionStatus("Complete");

            //
            // Complete response with fields dynamically generated from directory info.
            //

            return completeContainer(containerRequest, directory, path);

        } else { // Moving a Container

            //TODO:

            /*

            if (checkIfDirectoryFileExists(directory.getAbsolutePath())) {
                throw new IllegalArgumentException("Cannot move container '"
                                                   + containerRequest.getMove()
                                                   + "' to '"
                                                   + path
                                                   + "'; Destination already exists");
            }

            File sourceContainerFile = absoluteFile(containerRequest.getMove());

            if (!checkIfDirectoryFileExists(sourceContainerFile.getAbsolutePath())) {
                throw new NotFoundException("Path '"
                                            + directory.getAbsolutePath()
                                            + "' does not identify an existing container");
            }
            if (!isDirectory(sourceContainerFile.getAbsolutePath())) {
                throw new IllegalArgumentException("Path '"
                                                   + directory.getAbsolutePath()
                                                   + "' does not identify a container");
            }

            //
            // Move Container directory
            //

            renameDirectory(sourceContainerFile.getAbsolutePath(), directory.getAbsolutePath());
            */

            //
            // Move Container's Metadata .file
            //
            //File sourceContainerFieldsFile = getContainerFieldsFile(containerRequest.getMove());

            //renameFile(sourceContainerFieldsFile.getAbsolutePath(), containerFieldsFile.getAbsolutePath());

            //
            // Get the containers field's to return in response
            //

            //CDMIContainer movedContainer = getPersistedContainerFields(containerFieldsFile);
            CDMIContainer movedContainer = new CDMIContainer();

            //
            // If the request has a metadata field, replace any metadata filed in the source
            // Container
            //

            /*
            if (!containerRequest.getMetadata().isEmpty()) {

                try {
                    CDMIContainer movdCMd = new CDMIContainer();
                    if (useDB) {
                        movdCMd.fromJson(readMetadata(movedContainer.getObjectID()).getBytes(), true);
                    }
                    String cdmi_ctime = "never";
                    String cdmi_atime = "never";
                    String cdmi_mtime = "never";
                    String cdmi_size = "0";
                    FileAttributes attr = getAttributesByPath(path);
                    if (attr != null) {
                        cdmi_ctime = sdf.format(attr.getCreationTime());
                        cdmi_atime = sdf.format(attr.getAccessTime());
                        cdmi_mtime = sdf.format(attr.getModificationTime());
                        cdmi_size = String.valueOf(attr.getSize());
                    }
                    //OLD: String cdmi_ctime = movedContainer.getMetadata().get("cdmi_ctime");
                    //OLD: String cdmi_mtime = movedContainer.getMetadata().get("cdmi_mtime");
                    //OLD: String cdmi_atime = movedContainer.getMetadata().get("cdmi_atime");
                    //OLD: String cdmi_acount = movedContainer.getMetadata().get("cdmi_acount");
                    String cdmi_acount = movdCMd.getMetadata().get("cdmi_acount");
                    //OLD: String cdmi_mcount = movedContainer.getMetadata().get("cdmi_mcount");
                    String cdmi_mcount = movdCMd.getMetadata().get("cdmi_mcount");
                    //NEW:
                    List<HashMap<String, String>> cdmi_acl = movdCMd.getSubMetadata_ACL();

                    //OLD: movedContainer.setMetaData(containerRequest.getMetadata());
                    movdCMd.setMetadata(containerRequest.getMetadata());

                    try {
                        FileAttributes attr2 = new FileAttributes();
                        Date ctime = sdf.parse(cdmi_ctime);
                        Date atime = sdf.parse(cdmi_atime);
                        Date mtime = sdf.parse(cdmi_mtime);
                        long ctimeAsLong = ctime.getTime();
                        long atimeAsLong = atime.getTime();
                        long mtimeAsLong = mtime.getTime();
                        attr2.setCreationTime(ctimeAsLong);
                        attr2.setAccessTime(atimeAsLong);
                        attr2.setModificationTime(mtimeAsLong);
                        PnfsId id = new PnfsId(movdCMd.getPnfsID());
                        pnfsHandler.setFileAttributes(id, attr2);
                        movdCMd.setMetadata("cdmi_ctime", cdmi_ctime);
                        movdCMd.setMetadata("cdmi_atime", cdmi_atime);
                        movdCMd.setMetadata("cdmi_mtime", cdmi_mtime);
                    } catch (CacheException ex) {
                        Logger.getLogger(CDMIDataObjectDaoImpl.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    //OLD: movedContainer.getMetadata().put("cdmi_ctime", cdmi_ctime);
                    //OLD: movedContainer.getMetadata().put("cdmi_mtime", cdmi_mtime);
                    //OLD: movedContainer.getMetadata().put("cdmi_atime", cdmi_atime);
                    //OLD: movedContainer.getMetadata().put("cdmi_acount", cdmi_acount);
                    movdCMd.setMetadata("cdmi_acount", cdmi_acount);
                    //OLD: movedContainer.getMetadata().put("cdmi_mcount", cdmi_mcount);
                    movdCMd.setMetadata("cdmi_mcount", cdmi_mcount);
                    //NEW:
                    movdCMd.setMetadata("cdmi_size", cdmi_size);
                    movdCMd.setSubMetadata_ACL(cdmi_acl);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.debug("Exception while reading storage meta data: " + ex);
                    throw new IllegalArgumentException("Cannot read storage meta data, internal error : " + ex);
                }

                //
                // Write created or updated persisted fields out to the "." file
                //
                //containerRequest.getMetadata().clear();

                // write pseudo metadata file
                //if (!writeFile(containerFieldsFile.getAbsolutePath(), containerRequest.toJson(true))) {
                //    _log.debug("Exception while writing.");
                //    throw new IllegalArgumentException("Cannot write container fields file @"
                //                                       + path);
                //}

                // write real metadata to DB
                if (useDB) {
                    if (!writeMetadata(containerRequest.getObjectID(), cMd.metadataToJson(true))) {
                        _log.debug("Exception while writing to Mongo DB.");
                        throw new IllegalArgumentException("Cannot write metadata to table '"
                                                           + DB_MONGO_TABLE_STORAGE_SYS_METADATA + "' of MongoDB '"
                                                           + DB_MONGO_DATABASE_NAME);
                    }
                }

            }

            //
            // Transient fields
            //
            */

            //movedContainer.setCompletionStatus("Complete");
            movedContainer.setCompletionStatus("Incomplete");

            //
            // Complete response with fields dynamically generated from directory info.
            //

            return completeContainer(movedContainer, directory, path);
        }

    }

    //
    // For now this method supports both Container and Object delete.
    //
    // Improper requests directed at the root container are not routed here by
    // PathResource.
    //
    @Override
    public void deleteByPath(String path) {
        File directoryOrFile = absoluteFile(path);

        _log.debug("Delete container/object <path>: " + directoryOrFile.getAbsolutePath());

        //
        // Setup ISO-8601 Date
        //
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        //
        String objectID = "";
        CDMIContainer requestedContainer = new CDMIContainer();
        FileAttributes attr = getAttributesByPath(directoryOrFile.getAbsolutePath());
        if (attr != null) {
            pnfsId = attr.getPnfsId();
            if (pnfsId != null) {
                // update with real info
                _log.debug("CDMIContainerDao<Delete>, setPnfsID: " + pnfsId.toIdString());
                requestedContainer.setPnfsID(pnfsId.toIdString());
                requestedContainer.setMetadata("cdmi_ctime", sdf.format(attr.getCreationTime()));
                requestedContainer.setMetadata("cdmi_atime", sdf.format(attr.getAccessTime()));
                requestedContainer.setMetadata("cdmi_mtime", sdf.format(attr.getModificationTime()));
                requestedContainer.setMetadata("cdmi_size", String.valueOf(attr.getSize()));
                objectID = new IDConverter().toObjectID(pnfsId.toIdString());
                requestedContainer.setObjectID(objectID);
                _log.debug("CDMIContainerDao<Delete>, setObjectID: " + objectID);
            } else {
                _log.error("CDMIContainerDao<Delete>, Cannot read PnfsId from meta information, ObjectID will be empty");
            }
        } else {
            _log.error("CDMIContainerDao<Delete>, Cannot read meta information from directory or object: " + directoryOrFile.getAbsolutePath());
        }

        requestedContainer.setMetadata("cdmi_acount", "0");
        requestedContainer.setMetadata("cdmi_mcount", "0");
        // set default ACL
        List<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
        HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
        subMetadataEntry_ACL.put("acetype", "ALLOW");
        subMetadataEntry_ACL.put("identifier", "OWNER@");
        subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
        subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
        subMetadata_ACL.add(subMetadataEntry_ACL);
        requestedContainer.setSubMetadata_ACL(subMetadata_ACL);

        if (isExistingDirectory(directoryOrFile.getAbsolutePath())) {
            deleteRecursively(directoryOrFile.getAbsolutePath());
        } else {
            deleteFile(directoryOrFile.getAbsolutePath());
        }

    }

    //
    // Not Implemented
    //
    @Override
    public CDMIContainer findByObjectId(String objectId) {
        throw new UnsupportedOperationException("ContainerDaoImpl.findByObjectId()");
    }

    //
    //
    //
    @Override
    public CDMIContainer findByPath(String path) {

        if (listDirectoryHandler == null) {
            init();
        }

        _log.debug("In CDMIContainerDAO.findByPath : " + path);

        File directory = absoluteFile(path);

        if (!checkIfDirectoryFileExists(directory.getAbsolutePath())) {
            throw new NotFoundException("Path '"
                                        + directory.getAbsolutePath()
                                        + "' does not identify an existing container");
        }
        if (!checkIfDirectoryExists(directory.getAbsolutePath())) {
            throw new IllegalArgumentException("Path '"
                                               + directory.getAbsolutePath()
                                               + "' does not identify a container");
        }

        //
        // Setup ISO-8601 Date
        //
        Date now = new Date();
        long nowAsLong = now.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        CDMIContainer requestedContainer = new CDMIContainer();

        if (path != null) {

            //
            // Read the persisted container fields from the "." file
            //

            String objectID = "";
            int cowner = 0;
            ACL cacl = null;
            FileAttributes attr = getAttributesByPath(directory.getAbsolutePath());
            if (attr != null) {
                pnfsId = attr.getPnfsId();
                if (pnfsId != null) {
                    // update with real info
                    _log.debug("CDMIContainerDao<Read>, setPnfsID: " + pnfsId.toIdString());
                    requestedContainer.setPnfsID(pnfsId.toIdString());
                    requestedContainer.setMetadata("cdmi_ctime", sdf.format(attr.getCreationTime()));
                    requestedContainer.setMetadata("cdmi_atime", sdf.format(attr.getAccessTime()));
                    requestedContainer.setMetadata("cdmi_mtime", sdf.format(attr.getModificationTime()));
                    requestedContainer.setMetadata("cdmi_size", String.valueOf(attr.getSize()));
                    cowner = attr.getOwner();
                    cacl = attr.getAcl();
                    objectID = new IDConverter().toObjectID(pnfsId.toIdString());
                    requestedContainer.setObjectID(objectID);
                    _log.debug("CDMIContainerDao<Read>, setObjectID: " + objectID);
                } else {
                    _log.error("CDMIContainerDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
                }
            } else {
                _log.error("CDMIContainerDao<Read>, Cannot read meta information from directory: " + directory.getAbsolutePath());
            }

            //
            // Dynamically generate the default values
            //
            requestedContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
            requestedContainer.setDomainURI("/cdmi_domains/default_domain");

            requestedContainer.setMetadata("cdmi_acount", "0");
            requestedContainer.setMetadata("cdmi_mcount", "0");
            requestedContainer.setMetadata("cdmi_owner", String.valueOf(cowner));  //TODO
            if (cacl != null && !cacl.isEmpty()) {
                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                for (ACE ace : cacl.getList()) {
                    HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                    subMetadataEntry_ACL.put("acetype", ace.getType().name());
                    subMetadataEntry_ACL.put("identifier", ace.getWho().name());
                    subMetadataEntry_ACL.put("aceflags", String.valueOf(ace.getFlags()));
                    subMetadataEntry_ACL.put("acemask", String.valueOf(ace.getAccessMsk()));
                    subMetadata_ACL.add(subMetadataEntry_ACL);
                }
                requestedContainer.setSubMetadata_ACL(subMetadata_ACL);
            } else {
                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                subMetadataEntry_ACL.put("acetype", "ALLOW");
                subMetadataEntry_ACL.put("identifier", "OWNER@");
                subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                subMetadata_ACL.add(subMetadataEntry_ACL);
                requestedContainer.setSubMetadata_ACL(subMetadata_ACL);
            }

            //
            // Write created or updated persisted fields out to the "." file
            //

            // update meta information
            try {
                FileAttributes attr2 = new FileAttributes();
                attr2.setAccessTime(nowAsLong);
                pnfsHandler.setFileAttributes(pnfsId, attr2);
                requestedContainer.setMetadata("cdmi_atime", sdf.format(now));
            } catch (CacheException ex) {
                _log.error("CDMIContainerDao<Read>, Cannot update meta information for object with objectID " + requestedContainer.getObjectID());
            }

        } else {

            //
            // if this is the root container there is no "." metadata file up one level.
            // Dynamically generate the default values
            //

            requestedContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
            requestedContainer.setDomainURI("/cdmi_domains/default_domain");
        }

        return completeContainer(requestedContainer, directory, path);
    }

    //
    // Private Helper Methods
    //
    /**
     * <p>
     * Return a {@link File} instance for the container fields file object.
     * </p>
     *
     * @param path
     *            Path of the requested container.
     */
    private File getContainerFieldsFile(String path) {
        // path should be /<parent container name>/<container name>
        String[] tokens = path.split("[/]+");
        if (tokens.length < 1) {
            throw new BadRequestException("No object name in path <" + path + ">");
        }
        String containerName = tokens[tokens.length - 1];
        String containerFieldsFileName = "." + containerName;
        // piece together parent container name
        // FIXME : This is the kludge way !
        String parentContainerName = "";
        for (int i = 0; i <= tokens.length - 2; i++) {
            parentContainerName += tokens[i] + "/";
        }
        _log.debug("Path = " + path);
        _log.debug("Parent Container Name = "
                           + parentContainerName
                           + " Container Name == "
                           + containerName);


        File baseDirectory1, parentContainerDirectory, containerFieldsFile;
        try {
            _log.debug("baseDirectory = " + baseDirectoryName);
            baseDirectory1 = new File(baseDirectoryName + "/");
            System.out
                    .println("Base Directory Absolute Path = " + baseDirectory1.getAbsolutePath());
            parentContainerDirectory = new File(baseDirectory1, parentContainerName);
            //
            _log.debug("Parent Container Absolute Path = "
                               + parentContainerDirectory.getAbsolutePath());
            //
            containerFieldsFile = new File(parentContainerDirectory, containerFieldsFileName);
            _log.debug("Container Metadata File Path = "
                               + containerFieldsFile.getAbsolutePath());
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.error("Exception while building File objects: " + ex);
            throw new IllegalArgumentException("Cannot build Object @" + path + " error : " + ex);
        }
        return containerFieldsFile;
    }

    /**
     * <p>
     * Return a {@link Container} instance for the container fields.
     * </p>
     *
     * @param containerFieldsFile
     *            File object for the container fields file.
     */
    private CDMIContainer getPersistedContainerFields(File containerFieldsFile) {
        CDMIContainer containerFields = new CDMIContainer();
        try {
            byte[] inBytes = readFile(containerFieldsFile.getAbsolutePath());
            containerFields.fromJson(inBytes, true);
            String mds = new String(inBytes);
            _log.debug("Container fields read were:" + mds);
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.error("Exception while reading: " + ex);
            throw new IllegalArgumentException("Cannot read container fields file error: " + ex);
        }
        return containerFields;
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
    public File absoluteFile(String path) {
        if (path == null) {
            return baseDirectory();
        } else {
            return new File(baseDirectory(), path);
        }
    }

    private File baseDirectory = null;

    /**
     * <p>
     * Return a {@link File} instance for the base directory, erasing any previous content on first
     * use if the <code>recreate</code> flag has been set.
     * </p>
     *
     * @exception IllegalArgumentException
     *                if we cannot create the base directory
     */
    private File baseDirectory() {
        if (baseDirectory == null) {
            baseDirectory = new File(baseDirectoryName);
            if (recreate) {
                deleteRecursively(baseDirectory.getAbsolutePath());
                if (!baseDirectory.mkdirs()) {
                    throw new IllegalArgumentException("Cannot create base directory '"
                                                       + baseDirectoryName
                                                       + "'");
                }
            }
        }
        return baseDirectory;
    }

    /**
     * <p>
     * Return the {@link Container} identified by the specified <code>path</code>.
     * </p>
     *
     * @param container
     *            The requested container with persisted fields
     * @param directory
     *            Directory of the requested container
     * @param path
     *            Path of the requested container
     *
     * @exception NotFoundException
     *                if the specified path does not identify a valid resource
     * @exception IllegalArgumentException
     *                if the specified path identifies a data object instead of a container
     */
    private CDMIContainer completeContainer(CDMIContainer container, File directory, String path) {
        _log.debug("In ContainerDaoImpl.Container, path is: " + path);

        _log.debug("In ContainerDaoImpl.Container, absolute path is: "
                           + directory.getAbsolutePath());


        container.setObjectType("application/cdmi-container");

        //
        // Derive ParentURI
        //

        String parentURI = "/";

        if (path != null) {
            String[] tokens = path.split("[/]+");
            String containerName = tokens[tokens.length - 1];
            // FIXME : This is the kludge way !
            for (int i = 0; i <= tokens.length - 2; i++) {
                parentURI += tokens[i] + "/";
            }
            _log.debug("In ContainerDaoImpl.Container, ParentURI = "
                               + parentURI
                               + " Container Name = "
                               + containerName);
            // Check for illegal top level container names
            if (parentURI.matches("/") && containerName.startsWith("cdmi")) {
                throw new BadRequestException("Root container names must not start with cdmi");
            }
        }

        container.setParentURI(parentURI);

        //
        // Add children containers and/or objects representing subdirectories or
        // files
        //

        List<String> children = container.getChildren();

        for (Map.Entry<String, FileAttributes> entry : listDirectoriesFilesByPath(directory.getAbsolutePath()).entrySet()) {
            if (entry.getValue().getFileType() == DIR) {
                children.add(entry.getKey() + "/");
            } else {
                if (!entry.getKey().startsWith(".")) {
                    children.add(entry.getKey());
                }
            }
        }

        if (children.size() > 0) {
            // has children - set the range
            int lastindex = children.size() - 1;
            String childrange = "0-" + lastindex;
            container.setChildrenrange(childrange);
        }

        return container;
    }

    /**
     * <p>
     * (Deprecated)
     * Delete the specified directory, after first recursively deleting any contents within it.
     * </p>
     *
     * @param directory
     *            {@link File} identifying the directory to be deleted
     */
    private void recursivelyDelete(File directory) {
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                recursivelyDelete(file);
            } else {
                file.delete();
            }
        }
        directory.delete();
    }

    //

    @Override
    public boolean isContainer(String path) {

        if (listDirectoryHandler == null) {
            init();
        }

        if (path == null || isDirectory(path)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * DCache related stuff.
     */

    // Temp Helper Function
    private void init() {
        pnfsStub = CDMIDataTransfer.getPnfsStub();
        pnfsHandler = CDMIDataTransfer.getPnfsHandler();
        listDirectoryHandler = CDMIDataTransfer.getListDirectoryHandler();
        poolStub = CDMIDataTransfer.getPoolStub();
        poolMgrStub = CDMIDataTransfer.getPoolMgrStub();
        billingStub = CDMIDataTransfer.getBillingStub();
        baseDirectoryName = CDMIDataTransfer.getBaseDirectoryName();
    }

    //This function is necessary, otherwise the attributes and servletContext are not set.
    //It is called before afterStart() of the CellLifeCycleAware interface, which is wanted, too.
    //In other words: contextInitialized() must be called before afterStart().
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        _log.debug("Init CDMIContainerDaoImpl...");
        this.servletContext = servletContextEvent.getServletContext();
        this.pnfsStub = getCellStubAttribute();
        this.pnfsHandler = new PnfsHandler(pnfsStub);
        //this.listDirectoryHandler = new ListDirectoryHandler(pnfsHandler); //does not work, tested 100 times
        this.listDirectoryHandler = getListDirAttribute(); //it only works in this way, tested 100 times
        this.poolStub = getPoolAttribute();
        this.poolMgrStub = getPoolMgrAttribute();
        this.billingStub = getBillingAttribute();
        //Temp Helper Part
        if (baseDirectoryName != null) CDMIDataTransfer.setBaseDirectoryName(baseDirectoryName);
        CDMIDataTransfer.setPnfsStub(pnfsStub);
        CDMIDataTransfer.setPnfsHandler(pnfsHandler);
        CDMIDataTransfer.setListDirectoryHandler(listDirectoryHandler);
        CDMIDataTransfer.setPoolStub(poolStub);
        CDMIDataTransfer.setPoolMgrStub(poolMgrStub);
        CDMIDataTransfer.setBillingStub(billingStub);
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce)
    {
    }

    @Override
    public void afterStart()
    {
        _log.debug("Start CDMIContainerDaoImpl...");
    }

    @Override
    public void beforeStop()
    {
    }

    private CellStub getCellStubAttribute()  //tested, ok
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

    private ListDirectoryHandler getListDirAttribute()  //tested, ok
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

    private CellStub getPoolAttribute()  //tested, ok
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

    private CellStub getPoolMgrAttribute()  //tested, ok
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

    private CellStub getBillingAttribute()  //tested, ok
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
                result = parent;
            }
            if (parent.isEmpty()) {
                result = "/";
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

    private boolean isDirectory(String dirPath)
    {
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        return checkIfDirectoryExists(baseDirectoryName + tmpDirPath);
    }

    private boolean isExistingDirectory(String dirPath)
    {
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        return checkIfDirectoryExists(tmpDirPath);
    }

    private boolean checkIfDirectoryExists(String dirPath)
    {
        boolean result = false;
        String searchedItem = getItem(dirPath);
        List<String> listing = listDirectoriesByPath(getParentDirectory(dirPath));
        for (String dir : listing) {
            if (dir.compareTo(searchedItem) == 0) {
                result = true;
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

    private List<String> listDirectoriesByPath(String path)
    {
        List<String> result = new ArrayList<>();
        String tmpPath = addPrefixSlashToPath(path);
        Map<String, FileAttributes> listing = listDirectoriesFilesByPath(tmpPath);
        for (Map.Entry<String, FileAttributes> entry : listing.entrySet()) {
            if (entry.getValue().getFileType() == DIR) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    private List<String> listFilesByPath(String path)
    {
        List<String> result = new ArrayList<>();
        String tmpPath = addPrefixSlashToPath(path);
        Map<String, FileAttributes> listing = listDirectoriesFilesByPath(tmpPath);
        for (Map.Entry<String, FileAttributes> entry : listing.entrySet()) {
            if (entry.getValue().getFileType() == REGULAR) {
                result.add(entry.getKey());
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
            _log.warn("Directory and file listing for path '" + path + "' was not possible, internal error message: " + ex.getMessage());
        }
        return result;
    }

    private boolean createDirectory(String dirPath)
    {
        boolean result = false;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        try {
            pnfsHandler.createDirectories(new FsPath(tmpDirPath));
            result = true;
        } catch (CacheException ex) {
            _log.warn("Directory '" + dirPath + "' could not get created, internal error message: " + ex.getMessage());
        }
        return result;
    }

    private boolean renameDirectory(String dirPath, String newName)
    {
        boolean result = false;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        try {
            pnfsHandler.renameEntry(tmpDirPath, newName, true);
            result = true;
        } catch (CacheException ex) {
            _log.warn("Directory '" + dirPath + "' could not get renamed, internal error message: " + ex.getMessage());
        }
        return result;
    }

    private boolean deleteDirectory(String dirPath)
    {
        boolean result = false;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        try {
            pnfsHandler.deletePnfsEntry(tmpDirPath);
            result = true;
        } catch (CacheException ex) {
            _log.warn("Directory '" + dirPath + "' could not get deleted, internal error message: " + ex.getMessage());
        }
        return result;
    }

    private boolean renameFile(String filePath, String newName)
    {
        boolean result = false;
        String tmpFilePath = addPrefixSlashToPath(filePath);
        try {
            pnfsHandler.renameEntry(tmpFilePath, newName, true);
            result = true;
        } catch (CacheException ex) {
            _log.warn("File '" + filePath + "' could not get renamed, internal error message: " + ex.getMessage());
        }
        return result;
    }

    private boolean deleteFile(String filePath)
    {
        boolean result = false;
        String tmpFilePath = addPrefixSlashToPath(filePath);
        try {
            pnfsHandler.deletePnfsEntry(tmpFilePath);
            result = true;
        } catch (CacheException ex) {
            _log.warn("File '" + filePath + "' could not get deleted, internal error message: " + ex.getMessage());
        }
        return result;
    }

    private void deleteRecursively(String path)
    {
        String tmpPath = addPrefixSlashToPath(path);
        Map<String, FileAttributes> listing = listDirectoriesFilesByPath(tmpPath);
        for (Map.Entry<String, FileAttributes> entry : listing.entrySet()) {
            if (entry.getValue().getFileType() == REGULAR) {
                deleteFile(entry.getKey());
            } else {
                deleteRecursively(entry.getKey());
                //deleteDirectory(entry.getKey());
            }
        }
        deleteDirectory(tmpPath);
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

    //OLD:
    @Override
    public Container createByPath(String string, Container cntnr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
            CDMIDataTransfer.setData(data);
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
                    pnfsId = CDMIDataTransfer.getPnfsId();
                    creationTime = CDMIDataTransfer.getCreationTime();
                    accessTime = CDMIDataTransfer.getAccessTime();
                    changeTime = CDMIDataTransfer.getChangeTime();
                    modificationTime = CDMIDataTransfer.getModificationTime();
                    size = CDMIDataTransfer.getSize();
                    owner = CDMIDataTransfer.getOwner();
                    acl = CDMIDataTransfer.getACL();
                    fileType = CDMIDataTransfer.getFileType();
                    //transfer.killMover(2000, TimeUnit.MILLISECONDS);
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                _log.debug("TEST1W-isWrite:" + transfer.isWrite());
                if (pnfsId != null) _log.debug("TEST1W-pnfsId:" + pnfsId);
                _log.debug("TEST1W-creationTime:" + sdf.format(creationTime));
                _log.debug("TEST1W-accessTime:" + sdf.format(accessTime));
                _log.debug("TEST1W-changeTime:" + sdf.format(changeTime));
                _log.debug("TEST1W-modificationTime:" + sdf.format(modificationTime));
                _log.debug("TEST1W-size:" + size);
                _log.debug("TEST1W-owner:" + owner);
                if (acl != null) _log.debug("TEST1W-acl:" + acl.toString());
                if (acl != null) _log.debug("TEST1W-aclExtraFormat:" + acl.toExtraFormat());
                if (acl != null) _log.debug("TEST1W-aclNFSv4String:" + acl.toNFSv4String());
                if (acl != null) _log.debug("TEST1W-aclOrgString:" + acl.toOrgString());
                if (fileType != null) _log.debug("TEST1W-fileType:" + fileType.toString());
                _log.debug("TEST1W-data:" + data);
                result = true;
            } finally {
                if (result == false) {
                    //transfer.deleteNameSpaceEntry();
                }
            }
        } catch (CacheException | InterruptedException | UnknownHostException | ACLException ex) {
            _log.error("CDMIContainerDaoImpl, File could not become written, exception is: " + ex.getMessage());
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
                    result = CDMIDataTransfer.getDataAsBytes();
                    _log.debug("CDMIContainerDaoImpl received data: " + result.toString());
                }
                finally {
                    pnfsId = CDMIDataTransfer.getPnfsId();
                    creationTime = CDMIDataTransfer.getCreationTime();
                    accessTime = CDMIDataTransfer.getAccessTime();
                    changeTime = CDMIDataTransfer.getChangeTime();
                    modificationTime = CDMIDataTransfer.getModificationTime();
                    size = CDMIDataTransfer.getSize();
                    owner = CDMIDataTransfer.getOwner();
                    acl = CDMIDataTransfer.getACL();
                    fileType = CDMIDataTransfer.getFileType();
                    //transfer.killMover(2000, TimeUnit.MILLISECONDS);
                }
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                _log.debug("TEST1R-isWrite:" + transfer.isWrite());
                if (pnfsId != null) _log.debug("TEST1R-pnfsId:" + pnfsId);
                _log.debug("TEST1R-creationTime:" + sdf.format(creationTime));
                _log.debug("TEST1R-accessTime:" + sdf.format(accessTime));
                _log.debug("TEST1R-changeTime:" + sdf.format(changeTime));
                _log.debug("TEST1R-modificationTime:" + sdf.format(modificationTime));
                _log.debug("TEST1R-size:" + size);
                _log.debug("TEST1R-owner:" + owner);
                if (acl != null) _log.debug("TEST1R-acl:" + acl.toString());
                if (acl != null) _log.debug("TEST1R-aclExtraFormat:" + acl.toExtraFormat());
                if (acl != null) _log.debug("TEST1R-aclNFSv4String:" + acl.toNFSv4String());
                if (acl != null) _log.debug("TEST1R-aclOrgString:" + acl.toOrgString());
                if (fileType != null) _log.debug("TEST1R-fileType:" + fileType.toString());
                _log.debug("TEST1R-data:" + result.toString());
            } finally {
                if (result == null) {
                    //transfer.deleteNameSpaceEntry();
                }
            }
        } catch (CacheException | InterruptedException | UnknownHostException | ACLException ex) {
            _log.error("CDMIContainerDaoImpl, File could not become read, exception is: " + ex.getMessage());
        }
        return result;
    }

    //Minimum to write a file
    public void writeFileExample()
    {
        try {
            //The order of all commands is very important!
            String data = "Hello!";
            String filePath = "/disk/test2.txt";
            PnfsId id = null;
            Subject subject = Subjects.ROOT;
            CDMIDataTransfer.setData(data);
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
            transfer.createNameSpaceEntryWithParents();
            transfer.getPnfsId();
            transfer.selectPoolAndStartMover(null, TransferRetryPolicies.tryOncePolicy(5000));
        } catch (CacheException | InterruptedException | UnknownHostException ex) {
            _log.error("File could not become written, exception is: " + ex.getMessage());
        }
    }

    //Minimum to read a file
    public void readFileExample()
    {
        try {
            //The order of all commands is very important!
            String filePath = "/disk/test2.txt";
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
            transfer.readNameSpaceEntry();
            transfer.selectPoolAndStartMover(null, TransferRetryPolicies.tryOncePolicy(5000));
            String data = CDMIDataTransfer.getDataAsString();
            _log.debug("CDMIContainerDaoImpl received data: " + data);
        } catch (CacheException | InterruptedException | UnknownHostException ex) {
            _log.error("File could not become read, exception is: " + ex.getMessage());
        }
    }

}