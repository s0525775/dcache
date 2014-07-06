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
import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.AbstractCellComponent;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.*;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
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
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import java.security.Principal;
import java.text.ParseException;
import javax.security.auth.Subject;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.auth.Subjects;
import org.dcache.cdmi.filter.ContextHolder;
import org.dcache.cdmi.model.DcacheContainer;
import org.dcache.cdmi.util.IdConverter;
import org.snia.cdmiserver.exception.UnauthorizedException;

/* This class is dCache's DAO implementation class for SNIA's ContainerDao interface.
   Container represents a directory in the CDMI standard.
   This class contains all operations which are related to directory operations, such as get/create/update/delete a directory.
   Moving a directory is still not supported here, but this support will be added here very soon.
   Getting/Creating/Updating/Deleting a directory via ObjectID still isn't supported, the support shall still get added here.
   Since SNIA has found out that files and directories share some functions, some file functions are embedded in this class,
   therefore those functions are here and not in the DataObject class. For example, if you want to delete recursively or if
   you move a directory which contains files. Moving a directory will be implemented very soon. Deleting directories recursively
   (that means with content) is implemented already but it still doesn't work correctly and still needs to get fixed from my side
   (ToDo). At the moment you can only delete directories step by step. Jana
*/

/**
 * <p>
 * Concrete implementation of {@link ContainerDao} using the local filesystem as the backing store.
 * </p>
 */
public class DcacheContainerDao extends AbstractCellComponent
    implements ContainerDao, CellLifeCycleAware
{

    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcacheContainerDao.class);

    // Properties and Dependency Injection Methods by CDMI
    private String baseDirectoryName = null;
    private File baseDirectory = null;

    // Properties and Dependency Injection Methods by dCache
    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES =
        EnumSet.of(PNFSID, CREATION_TIME, ACCESS_TIME, CHANGE_TIME, MODIFICATION_TIME, TYPE, SIZE, MODE, ACL, OWNER, OWNER_GROUP);
    private CellStub pnfsStub;
    private CellStub billingStub;
    private PnfsHandler pnfsHandler;
    private ListDirectoryHandler listDirectoryHandler;
    private boolean isAnonymousListingAllowed;
    private String realm = "dCache";

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
        _log.trace("BaseDirectory(C)={}", baseDirectoryName);
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
     * Returns the current Subject of the calling thread.
     */
    private static Subject getSubject()
    {
        return Subject.getSubject(ContextHolder.get());
    }

    public void setAnonymousListing(boolean isAllowed)
    {
        this.isAnonymousListingAllowed = isAllowed;
    }

    public boolean isAnonymousListing()
    {
        return this.isAnonymousListingAllowed;
    }

    // ContainerDao Methods invoked from PathResource
    @Override
    public DcacheContainer createByPath(String path, Container containerRequest)
    {
        try {
            File directory = absoluteFile(path);
            _log.trace("Create container, path={}", directory.getAbsolutePath());

            // Setup ISO-8601 Date
            Date now = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            DcacheContainer newContainer = (DcacheContainer) containerRequest;

            Subject subject = getSubject();
            if ((subject == null) || Subjects.isNobody(subject)) {
                throw new UnauthorizedException("Access denied", realm);
            }

            if (newContainer.getMove() == null) { // This is a normal Create or Update

                if (!checkIfDirectoryFileExists(subject, directory.getAbsolutePath())) { // Create
                    _log.trace("<Container Create>");
                    String base =  removeSlashesFromPath(baseDirectoryName);
                    String parent = removeSlashesFromPath(getParentDirectory(directory.getAbsolutePath()));

                    if (!base.equals(parent)) {
                        if (!isUserAllowed(subject, parent)) {
                            throw new UnauthorizedException("Access denied", realm);
                        }
                    }

                    FileAttributes attributes = createDirectory(subject, directory.getAbsolutePath());

                    PnfsId pnfsId = null;
                    String objectId = "";
                    ACL cacl = null;
                    if (attributes != null) {
                        pnfsId = attributes.getPnfsId();
                        if (pnfsId != null) {
                            newContainer.setPnfsID(pnfsId.toIdString());
                            newContainer.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                            newContainer.setMetadata("cdmi_atime", sdf.format(attributes.getAccessTime()));
                            newContainer.setMetadata("cdmi_mtime", sdf.format(attributes.getModificationTime()));
                            newContainer.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                            newContainer.setMetadata("cdmi_owner", String.valueOf(attributes.getOwner()));
                            cacl = attributes.getAcl();
                            objectId = new IdConverter().toObjectID(pnfsId.toIdString());
                            newContainer.setObjectID(objectId);
                            _log.trace("DcacheContainerDao<Create>, setObjectID={}", objectId);

                            newContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
                            if (newContainer.getDomainURI() == null) {
                                newContainer.setDomainURI("/cdmi_domains/default_domain");
                            }
                            newContainer.setMetadata("cdmi_ctime", sdf.format(now));
                            newContainer.setMetadata("cdmi_acount", "0");
                            newContainer.setMetadata("cdmi_mcount", "0");

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
                                newContainer.setSubMetadata_ACL(subMetadata_ACL);
                            } else {
                                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                subMetadataEntry_ACL.put("acetype", "ALLOW");
                                subMetadataEntry_ACL.put("identifier", "OWNER@");
                                subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                                subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                                subMetadata_ACL.add(subMetadataEntry_ACL);
                                newContainer.setSubMetadata_ACL(subMetadata_ACL);
                            }

                            // update meta information
                            try {
                                FileAttributes attr = new FileAttributes();
                                Date ctime = sdf.parse(newContainer.getMetadata().get("cdmi_ctime"));
                                Date atime = sdf.parse(newContainer.getMetadata().get("cdmi_atime"));
                                Date mtime = sdf.parse(newContainer.getMetadata().get("cdmi_mtime"));
                                long ctimeAsLong = ctime.getTime();
                                long atimeAsLong = atime.getTime();
                                long mtimeAsLong = mtime.getTime();
                                attr.setCreationTime(ctimeAsLong);
                                attr.setAccessTime(atimeAsLong);
                                attr.setModificationTime(mtimeAsLong);
                                PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                                pnfs.setFileAttributes(pnfsId, attr);
                            } catch (CacheException | ParseException ex) {
                                _log.error("DcacheContainerDao<Update>, Cannot update meta information for object with objectID {}", newContainer.getObjectID());
                            }
                            containerRequest.setCompletionStatus("Complete");
                            return completeContainer(subject, newContainer, directory, path);
                        } else {
                            _log.error("DcacheContainerDao<Create>, Cannot read PnfsId from meta information, ObjectID will be empty");
                            return null;
                        }
                    } else {
                        throw new IllegalArgumentException("Error while creating container '" + path + "', no attributes available.");
                    }

                } else { // Updating Container
                    _log.trace("<Container Update>");

                    if (!isUserAllowed(subject, directory.getAbsolutePath())) {
                        throw new UnauthorizedException("Access denied", realm);
                    }

                    DcacheContainer currentContainer = new DcacheContainer();
                    FileAttributes attributes = getAttributesByPath(subject, directory.getAbsolutePath());

                    PnfsId pnfsId = null;
                    String objectID = "";
                    ACL cacl = null;
                    if (attributes != null) {
                        pnfsId = attributes.getPnfsId();
                        if (pnfsId != null) {
                            currentContainer.setPnfsID(pnfsId.toIdString());
                            currentContainer.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                            currentContainer.setMetadata("cdmi_atime", sdf.format(attributes.getAccessTime()));
                            currentContainer.setMetadata("cdmi_mtime", sdf.format(attributes.getModificationTime()));
                            currentContainer.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                            currentContainer.setMetadata("cdmi_owner", String.valueOf(attributes.getOwner()));
                            cacl = attributes.getAcl();
                            objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                            currentContainer.setObjectID(objectID);
                            _log.trace("DcacheContainerDao<Update>, setObjectID={}", objectID);

                            currentContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
                            if (currentContainer.getDomainURI() == null) {
                                currentContainer.setDomainURI("/cdmi_domains/default_domain");
                            }
                            currentContainer.setMetadata("cdmi_acount", "0");
                            currentContainer.setMetadata("cdmi_mcount", "0");
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
                            newContainer.setPnfsID(pnfsId.toIdString());
                            newContainer.setObjectID(objectID);
                            newContainer.setCapabilitiesURI(currentContainer.getCapabilitiesURI());
                            newContainer.setDomainURI(currentContainer.getDomainURI());

                            //forth-and-back update
                            for (String key : newContainer.getMetadata().keySet()) {
                                currentContainer.setMetadata(key, newContainer.getMetadata().get(key));
                            }
                            for (String key : currentContainer.getMetadata().keySet()) {
                                newContainer.setMetadata(key, currentContainer.getMetadata().get(key));
                            }
                            newContainer.setMetadata("cdmi_mtime", sdf.format(now));

                            newContainer.setSubMetadata_ACL(currentContainer.getSubMetadata_ACL());
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
                                newContainer.setSubMetadata_ACL(subMetadata_ACL);
                            } else {
                                ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                                HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                                subMetadataEntry_ACL.put("acetype", "ALLOW");
                                subMetadataEntry_ACL.put("identifier", "OWNER@");
                                subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                                subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                                subMetadata_ACL.add(subMetadataEntry_ACL);
                                newContainer.setSubMetadata_ACL(subMetadata_ACL);
                            }

                            // update meta information
                            try {
                                FileAttributes attr = new FileAttributes();
                                Date ctime = sdf.parse(newContainer.getMetadata().get("cdmi_ctime"));
                                Date atime = sdf.parse(newContainer.getMetadata().get("cdmi_atime"));
                                Date mtime = sdf.parse(newContainer.getMetadata().get("cdmi_mtime"));
                                long ctimeAsLong = ctime.getTime();
                                long atimeAsLong = atime.getTime();
                                long mtimeAsLong = mtime.getTime();
                                attr.setCreationTime(ctimeAsLong);
                                attr.setAccessTime(atimeAsLong);
                                attr.setModificationTime(mtimeAsLong);
                                PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                                pnfs.setFileAttributes(pnfsId, attr);
                            } catch (CacheException | ParseException ex) {
                                _log.error("DcacheContainerDao<Update>, Cannot update meta information for object with objectID {}", newContainer.getObjectID());
                                return null;
                            }
                            containerRequest.setCompletionStatus("Complete");
                            return completeContainer(subject, newContainer, directory, path);

                        } else {
                            _log.error("DcacheContainerDao<Update>, Cannot read PnfsId from meta information, ObjectID will be empty");
                            return null;
                        }
                    } else {
                        _log.error("DcacheContainerDao<Update>, Cannot read meta information from directory {}", directory.getAbsolutePath());
                        return null;
                    }
                }
            } else { // Moving a Container
                _log.trace("<Container Move>");
                //TODO: This part is still in process.
                if (!isUserAllowed(subject, directory.getAbsolutePath())) {
                    throw new UnauthorizedException("Access denied", realm);
                }

                DcacheContainer movedContainer = new DcacheContainer();

                movedContainer.setCompletionStatus("Incomplete");
                // Complete response with fields dynamically generated from directory info.
                return completeContainer(subject, movedContainer, directory, path);
            }
        } catch (CacheException ex) {
            return null;
        }

    }

    //
    // For now this method supports both Container and Object delete.
    //
    // Improper requests directed at the root container are not routed here by
    // PathResource.
    //
    @Override
    public void deleteByPath(String path)
    {
        try {
            File directoryOrFile;
            directoryOrFile = absoluteFile(path);
            _log.trace("Delete container/object, path={}", directoryOrFile.getAbsolutePath());

            Subject subject = getSubject();
            if ((subject == null) || Subjects.isNobody(subject)) {
                throw new UnauthorizedException("Access denied", realm);
            }

            if (!isUserAllowed(subject, directoryOrFile.getAbsolutePath())) {
                throw new UnauthorizedException("Access denied", realm);
            }

            // Setup ISO-8601 Date
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            PnfsId pnfsId = null;
            String objectID = "";
            DcacheContainer requestedContainer = new DcacheContainer();
            try {
                FileAttributes attributes = getAttributesByPath(subject, directoryOrFile.getAbsolutePath());
                if (attributes != null) {
                    pnfsId = attributes.getPnfsId();
                    if (pnfsId != null) {
                        requestedContainer.setPnfsID(pnfsId.toIdString());
                        requestedContainer.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                        requestedContainer.setMetadata("cdmi_atime", sdf.format(attributes.getAccessTime()));
                        requestedContainer.setMetadata("cdmi_mtime", sdf.format(attributes.getModificationTime()));
                        requestedContainer.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                        objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                        requestedContainer.setObjectID(objectID);
                        _log.trace("DcacheContainerDao<Delete>, setObjectID={}", objectID);
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
                        if (isExistingDirectory(subject, directoryOrFile.getAbsolutePath())) {
                            deleteDirectory(subject, pnfsId, directoryOrFile.getAbsolutePath());
                        } else {
                            deleteFile(pnfsId, directoryOrFile.getAbsolutePath());
                        }
                    } else {
                        _log.error("DcacheContainerDao<Delete>, Cannot read PnfsId from meta information, ObjectID will be empty");
                    }
                } else {
                    _log.error("DcacheContainerDao<Delete>, Cannot read meta information from directory or object {}", directoryOrFile.getAbsolutePath());
                }
            } catch (CacheException ex) {
                _log.error("DcacheContainerDao<Delete>, Cannot read meta information from directory or object {}", directoryOrFile.getAbsolutePath());
            } catch (Exception ex) {
                _log.error("DcacheContainerDao<Delete>, Cannot delete directory or object {}", directoryOrFile.getAbsolutePath());
            }
        } catch (CacheException ex) {
            _log.error("DcacheContainerDao<Delete>, An unknown error occured: more info: {}", ex);
        }
    }

    @Override
    public DcacheContainer findByObjectId(String objectId)
    {
        throw new UnsupportedOperationException("DcacheContainerDao.findByObjectId()");
    }

    @Override
    public DcacheContainer findByPath(String path)
    {
        _log.trace("In DcacheContainerDAO.findByPath, Path={}", path);

        try {
            DcacheContainer requestedContainer = new DcacheContainer();
            Subject subject = getSubject();
            File directory;
            directory = absoluteFile(path);

            if (path != null) {
                if (!isAnonymousListingAllowed && (subject == null) && Subjects.isNobody(subject)) {
                    throw new UnauthorizedException("Access denied", realm);
                }

                if (!isUserAllowed(subject, directory.getAbsolutePath())) {
                    throw new UnauthorizedException("Access denied", realm);
                }

                if (!checkIfDirectoryFileExists(subject, directory.getAbsolutePath())) {
                    throw new NotFoundException("Path '"
                            + directory.getAbsolutePath()
                            + "' does not identify an existing container");
                }
                if (!checkIfDirectoryExists(subject, directory.getAbsolutePath())) {
                    throw new IllegalArgumentException("Path '"
                            + directory.getAbsolutePath()
                            + "' does not identify a container");
                }

                // Setup ISO-8601 Date
                Date now = new Date();
                long nowAsLong = now.getTime();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

                FileAttributes attributes = getAttributesByPath(subject, directory.getAbsolutePath());

                PnfsId pnfsId = null;
                String objectID = "";
                int cowner = 0;
                ACL cacl = null;
                if (attributes != null) {
                    pnfsId = attributes.getPnfsId();
                    if (pnfsId != null) {
                        requestedContainer.setPnfsID(pnfsId.toIdString());
                        requestedContainer.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                        requestedContainer.setMetadata("cdmi_atime", sdf.format(attributes.getAccessTime()));
                        requestedContainer.setMetadata("cdmi_mtime", sdf.format(attributes.getModificationTime()));
                        requestedContainer.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                        cowner = attributes.getOwner();
                        cacl = attributes.getAcl();
                        objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                        requestedContainer.setObjectID(objectID);
                        _log.trace("DcacheContainerDao<Read>, setObjectID={}", objectID);

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

                        // update meta information
                        try {
                            FileAttributes attr2 = new FileAttributes();
                            attr2.setAccessTime(nowAsLong);
                            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                            pnfs.setFileAttributes(pnfsId, attr2);
                            requestedContainer.setMetadata("cdmi_atime", sdf.format(now));
                        } catch (CacheException ex) {
                            _log.error("DcacheContainerDao<Read>, Cannot update meta information for object with objectID {}", requestedContainer.getObjectID());
                        }
                    } else {
                        _log.error("DcacheContainerDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
                    }
                } else {
                    _log.error("DcacheContainerDao<Read>, Cannot read meta information from directory {}", directory.getAbsolutePath());
                }
            } else {
                // if this is the root container there is no "." metadata file up one level.
                // Dynamically generate the default values
                requestedContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
                requestedContainer.setDomainURI("/cdmi_domains/default_domain");
            }
            return completeContainer(subject, requestedContainer, directory, path);
        } catch (CacheException ex) {
            ex.printStackTrace();
            return null;
        }
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
    public File absoluteFile(String path) throws CacheException
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
     *
     * @exception IllegalArgumentException
     *                if we cannot create the base directory
     */
    private File baseDirectory() throws CacheException //TODO!!!
    {
        if (baseDirectory == null) {
            baseDirectory = new File(baseDirectoryName);
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
    private DcacheContainer completeContainer(Subject subject, DcacheContainer container, File directory, String path) throws CacheException
    {
        _log.trace("In DcacheContainerDao.Container, Path={}", path);
        _log.trace("In DcacheContainerDao.Container, AbsolutePath={}", directory.getAbsolutePath());
        container.setObjectType("application/cdmi-container");

        // Derive ParentURI
        String parentURI = "/";

        if (path != null) {
            String[] tokens = path.split("[/]+");
            String containerName = tokens[tokens.length - 1];
            // FIXME : This is the kludge way !
            for (int i = 0; i <= tokens.length - 2; i++) {
                parentURI += tokens[i] + "/";
            }
            _log.trace("In DcacheContainerDao.Container, ParentURI={}, Container Name={}", parentURI, containerName);
            // Check for illegal top level container names
            if (parentURI.matches("/") && containerName.startsWith("cdmi")) {
                throw new BadRequestException("Root container names must not start with cdmi");
            }
        }

        container.setParentURI(parentURI);

        // Add children containers and/or objects representing subdirectories or files
        List<String> children = container.getChildren();

        if (isAnonymousListingAllowed || ((subject != null) && !Subjects.isNobody(subject))) {
            for (Map.Entry<String, FileAttributes> entry : listDirectoriesFilesByPath(subject, directory.getAbsolutePath()).entrySet()) {
                if (entry.getValue().getFileType() == DIR) {
                    children.add(entry.getKey() + "/");
                } else {
                    if (!entry.getKey().startsWith(".")) {
                        children.add(entry.getKey());
                    }
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
     * Returns true if object is a container.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     * @return
     */
    @Override
    public boolean isContainer(String path)
    {
        try {
            return path == null || isDirectory(getSubject(), path);
        } catch (CacheException ex) {
            return false;
        }
    }

    /**
     * DCache related stuff.
     */
    @Override
    public void afterStart()
    {
        _log.trace("Start DcacheContainerDao");
    }

    @Override
    public void beforeStop()
    {
    }

    /**
     * <p>
     * Checks if a user is allowed to access a file path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private boolean isUserAllowed(Subject subject, String path) throws CacheException
    {
        boolean result = false;
        String tmpPath = addPrefixSlashToPath(path);
        PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
        FileAttributes attributes = pnfs.getFileAttributes(tmpPath, REQUIRED_ATTRIBUTES);
        System.out.println(path);
        System.out.println(Subjects.getUid(subject));
        System.out.println(attributes.getOwner());
        if (Subjects.getUid(subject) == attributes.getOwner()) {
            result = true;
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
                result = parent;
                if (parent.isEmpty()) {
                    result = "/";
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
     * Returns true if object is a directory.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean isDirectory(Subject subject, String dirPath) throws CacheException
    {
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        return checkIfDirectoryExists(subject, baseDirectoryName + tmpDirPath);
    }

    /**
     * <p>
     * Returns true if a Directory exists in a specific path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean isExistingDirectory(Subject subject, String dirPath) throws CacheException
    {
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        return checkIfDirectoryExists(subject, tmpDirPath);
    }

    /**
     * <p>
     * Checks if a Directory exists in a specific path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean checkIfDirectoryExists(Subject subject, String dirPath) throws CacheException
    {
        boolean result = false;
        String searchedItem = getItem(dirPath);
        List<String> listing = listDirectoriesByPath(subject, getParentDirectory(dirPath));
        for (String dir : listing) {
            if (dir.compareTo(searchedItem) == 0) {
                result = true;
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
    private boolean checkIfDirectoryFileExists(Subject subject, String dirPath) throws CacheException
    {
        boolean result = false;
        String searchedItem = getItem(dirPath);
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        Map<String, FileAttributes> listing = listDirectoriesFilesByPath(subject, getParentDirectory(tmpDirPath));
        for (Map.Entry<String, FileAttributes> entry : listing.entrySet()) {
            if (entry.getKey().compareTo(searchedItem) == 0) {
                result = true;
            }
        }
        return result;
    }

    /**
     * <p>
     * Lists all Directories in a specific path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private List<String> listDirectoriesByPath(Subject subject, String path) throws CacheException
    {
        List<String> result = new ArrayList<>();
        String tmpPath = addPrefixSlashToPath(path);
        Map<String, FileAttributes> listing = listDirectoriesFilesByPath(subject, tmpPath);
        for (Map.Entry<String, FileAttributes> entry : listing.entrySet()) {
            if (entry.getValue().getFileType() == DIR) {
                result.add(entry.getKey());
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
    private FileAttributes getAttributesByPnfsId(Subject subject, PnfsId pnfsid) throws CacheException
    {
        FileAttributes result = null;
        PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
        result = pnfs.getFileAttributes(pnfsid, REQUIRED_ATTRIBUTES);
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
    private FileAttributes getAttributesByPath(Subject subject, String path) throws CacheException
    {
        FileAttributes result = null;
        String tmpDirPath = addPrefixSlashToPath(path);
        PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
        result = pnfs.getFileAttributes(tmpDirPath, REQUIRED_ATTRIBUTES);
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
    private Map<String, FileAttributes> listDirectoriesFilesByPath(Subject subject, String path) throws CacheException
    {
        String tmpPath = addPrefixSlashToPath(path);
        FsPath fsPath = new FsPath(tmpPath);
        Map<String, FileAttributes> result = new HashMap<>();
        try {
            listDirectoryHandler.printDirectory(subject, new ListPrinter(result), fsPath, null, Range.<Integer>all());
        } catch (InterruptedException | CacheException ex) {
            _log.warn("Directory and file listing for path '{}' was not possible, {}", path, ex.getMessage());
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
    private FileAttributes createDirectory(Subject subject, String dirPath)
    {
        FileAttributes result = null;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            PnfsCreateEntryMessage reply = pnfs.createPnfsDirectory(tmpDirPath);
            result = pnfs.getFileAttributes(reply.getPnfsId(), REQUIRED_ATTRIBUTES);
        } catch (CacheException ex) {
            _log.warn("Directory '{}' could not get created, {}", dirPath, ex.getMessage());
        }
        return result;
    }

    /**
     * <p>
     * Deletes a Directory in a specific file path. Needed by function deleteRecursively.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean deleteDirectory(Subject subject, PnfsId pnfsid, String dirPath) throws Exception
    {
        boolean result = false;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        if (dirPath.isEmpty()) {
            throw new Exception("Permission denied");
        }
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            pnfs.deletePnfsEntry(pnfsid, tmpDirPath, EnumSet.of(DIR));
            result = true;
        } catch (CacheException ex) {
            _log.warn("Directory '{}' could not get deleted, {}", dirPath, ex.getMessage());
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
    private boolean deleteFile(PnfsId pnfsid, String filePath)
    {
        boolean result = false;
        String tmpFilePath = addPrefixSlashToPath(filePath);
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, getSubject());
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
     * Adds a prefix slash to a directory path if there isn't a slash already.
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

}