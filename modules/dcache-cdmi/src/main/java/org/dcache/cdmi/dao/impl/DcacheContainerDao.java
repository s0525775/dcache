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
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotInTrashCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
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
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.DoorRequestInfoMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import java.text.ParseException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.security.auth.Subject;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AceFlags;
import org.dcache.auth.Subjects;
import org.dcache.cdmi.exception.MethodNotAllowedException;
import org.dcache.cdmi.exception.ServerErrorException;
import org.dcache.cdmi.filter.ContextHolder;
import org.dcache.cdmi.model.DcacheContainer;
import org.dcache.cdmi.util.AceConverter;
import org.dcache.cdmi.util.IdConverter;
import org.dcache.util.list.DirectoryStream;
import org.snia.cdmiserver.exception.ConflictException;
import org.snia.cdmiserver.exception.ForbiddenException;
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
        EnumSet.of(PNFSID, CREATION_TIME, ACCESS_TIME, CHANGE_TIME, MODIFICATION_TIME, TYPE, SIZE, MODE, ACL, OWNER, OWNER_GROUP, STORAGEINFO);
    private CellStub pnfsStub;
    private CellStub billingStub;
    private PnfsHandler pnfsHandler;
    private ListDirectoryHandler listDirectoryHandler;
    private boolean isAnonymousListingAllowed;
    private String realm = "dCache";
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
     * Returns true if access to path is allowed through the WebDAV
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

    // ContainerDao Methods invoked from PathResource
    @Override
    public DcacheContainer createByPath(String path, Container containerRequest)
    {
        boolean objectIdPath = false;
        File directory = absoluteFile(path);
        _log.trace("Create container, path={}", directory.getAbsolutePath());
        if (path != null) {
            // Setup ISO-8601 Date
            Date now = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            DcacheContainer newContainer = (DcacheContainer) containerRequest;

            Subject subject = getSubject();
            if ((subject == null) || Subjects.isNobody(subject)) {
                throw new ForbiddenException("Permission denied");
            }

            String checkPath = path;
            if (path.startsWith("cdmi_objectid/") || path.startsWith("/cdmi_objectid/")) {
                String objectId = "";
                String restPath = "";
                String tempPath = "";
                objectIdPath = true;
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
            directory = absoluteFile(checkPath);

            if (newContainer.getMove() == null) { // This is a normal Create or Update

                if (!checkIfDirectoryFileExists(subject, directory.getAbsolutePath())) { // Create

                    _log.trace("<Container Create>");
                    if (objectIdPath) {
                        throw new MethodNotAllowedException("This method is not supported yet.");
                    }

                    String base =  removeSlashesFromPath(baseDirectoryName);
                    String parent = removeSlashesFromPath(getParentDirectory(directory.getAbsolutePath()));

                    if (!parent.equals(base)) {
                        if (!isUserAllowed(subject, parent)) {
                            throw new ForbiddenException("Permission denied: " + parent);
                        }
                    }

                    FileAttributes attributes = createDirectory(subject, directory.getAbsolutePath());

                    PnfsId pnfsId = null;
                    String objectId = "";
                    ACL cacl = null;
                    if (attributes != null) {
                        pnfsId = attributes.getPnfsId();
                        if (pnfsId != null) {
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
                            if (containerRequest.getDomainURI() == null) {
                                newContainer.setDomainURI("/cdmi_domains/default_domain");
                            } else {
                                newContainer.setDomainURI(containerRequest.getDomainURI());
                            }
                            newContainer.setMetadata("cdmi_ctime", sdf.format(now));
                            newContainer.setMetadata("cdmi_acount", "0");
                            newContainer.setMetadata("cdmi_mcount", "0");

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

                            containerRequest.setCompletionStatus("Complete");
                            return completeContainer(subject, newContainer, directory, checkPath);
                        } else {
                            throw new BadRequestException("Error while creating container '" + checkPath + "', no PnfsId set.");
                        }
                    } else {
                        throw new BadRequestException("Error while creating container '" + checkPath + "', no attributes available.");
                    }

                } else { // Updating Container
                    _log.trace("<Container Update>");

                    if (!isUserAllowed(subject, directory.getAbsolutePath())) {
                        throw new ForbiddenException("Permission denied");
                    }

                    DcacheContainer currentContainer = new DcacheContainer();
                    FileAttributes attributes = getAttributes(subject, directory.getAbsolutePath());

                    PnfsId pnfsId = null;
                    String objectId = "";
                    ACL cacl = null;
                    if (attributes != null) {
                        pnfsId = attributes.getPnfsId();
                        if (pnfsId != null) {
                            currentContainer.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                            currentContainer.setMetadata("cdmi_atime", sdf.format(attributes.getAccessTime()));
                            currentContainer.setMetadata("cdmi_mtime", sdf.format(attributes.getModificationTime()));
                            currentContainer.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                            currentContainer.setMetadata("cdmi_owner", String.valueOf(attributes.getOwner()));
                            cacl = attributes.getAcl();
                            objectId = new IdConverter().toObjectID(pnfsId.toIdString());
                            currentContainer.setObjectID(objectId);
                            _log.trace("DcacheContainerDao<Update>, setObjectID={}", objectId);

                            currentContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
                            if (containerRequest.getDomainURI() == null) {
                                currentContainer.setDomainURI("/cdmi_domains/default_domain");
                            } else {
                                currentContainer.setDomainURI(containerRequest.getDomainURI());
                            }
                            currentContainer.setMetadata("cdmi_acount", "0");
                            currentContainer.setMetadata("cdmi_mcount", "0");
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
                            newContainer.setObjectID(objectId);
                            newContainer.setCapabilitiesURI(currentContainer.getCapabilitiesURI());
                            newContainer.setDomainURI(currentContainer.getDomainURI());

                            //forth-and-back update
                            for (String key : newContainer.getMetadata().keySet()) {
                                currentContainer.setMetadata(key, newContainer.getMetadata().get(key));
                            }
                            for (String key : currentContainer.getMetadata().keySet()) {
                                newContainer.setMetadata(key, currentContainer.getMetadata().get(key));
                            }
                            newContainer.setMetadata("cdmi_atime", sdf.format(now));
                            newContainer.setMetadata("cdmi_mtime", sdf.format(now));

                            newContainer.setSubMetadata_ACL(currentContainer.getSubMetadata_ACL());
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
                                Date atime = sdf.parse(newContainer.getMetadata().get("cdmi_atime"));
                                Date mtime = sdf.parse(newContainer.getMetadata().get("cdmi_mtime"));
                                long atimeAsLong = atime.getTime();
                                long mtimeAsLong = mtime.getTime();
                                attr.setAccessTime(atimeAsLong);
                                attr.setModificationTime(mtimeAsLong);
                                PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                                pnfs.setFileAttributes(pnfsId, attr);
                            } catch (CacheException | ParseException ex) {
                                _log.error("DcacheContainerDao<Update>, Cannot update meta information for object with objectID {}", newContainer.getObjectID());
                            }
                            containerRequest.setCompletionStatus("Complete");
                            return completeContainer(subject, newContainer, directory, checkPath);

                        } else {
                            throw new BadRequestException("Error while updating container '" + checkPath + "', no PnfsId set.");
                        }
                    } else {
                        throw new BadRequestException("Error while updating container '" + checkPath + "', no attributes available.");
                    }
                }
            } else { // Moving/Renaming a Container
                _log.trace("<Container Move>");
                if (objectIdPath) {
                    throw new MethodNotAllowedException("This method is not supported yet.");
                }

                File sourceContainerFile = absoluteFile(containerRequest.getMove());

                if (!isUserAllowed(subject, sourceContainerFile.getAbsolutePath())) {
                    throw new ForbiddenException("Permission denied");
                }

                String base =  removeSlashesFromPath(baseDirectoryName);
                String parent = removeSlashesFromPath(getParentDirectory(directory.getAbsolutePath()));

                if (!checkIfDirectoryFileExists(subject, directory.getAbsolutePath())) {
                    if (!base.equals(parent)) {
                        if (!isUserAllowed(subject, parent)) {
                            throw new ForbiddenException("Permission denied");
                        }
                    }
                } else {
                    throw new ConflictException("Cannot move container '" + containerRequest.getMove()
                                                   + "' to '" + path + "'; Destination already exists");
                }
                if (!checkIfDirectoryFileExists(subject, sourceContainerFile.getAbsolutePath())) {
                    throw new BadRequestException("Path '" + sourceContainerFile.getAbsolutePath()
                                                 + "' does not identify an existing container");
                }

                DcacheContainer movedContainer = new DcacheContainer();
                FileAttributes attributes = renameDirectory(subject, sourceContainerFile.getAbsolutePath(), directory.getAbsolutePath());

                PnfsId pnfsId = null;
                String objectId = "";
                ACL cacl = null;
                if (attributes != null) {
                    pnfsId = attributes.getPnfsId();
                    if (pnfsId != null) {
                        for (String key : containerRequest.getMetadata().keySet()) {
                            movedContainer.setMetadata(key, containerRequest.getMetadata().get(key));
                        }
                        movedContainer.setMetadata("cdmi_ctime", sdf.format(attributes.getCreationTime()));
                        movedContainer.setMetadata("cdmi_atime", sdf.format(attributes.getAccessTime()));
                        movedContainer.setMetadata("cdmi_mtime", sdf.format(attributes.getModificationTime()));
                        movedContainer.setMetadata("cdmi_size", String.valueOf(attributes.getSize()));
                        movedContainer.setMetadata("cdmi_owner", String.valueOf(attributes.getOwner()));
                        cacl = attributes.getAcl();
                        objectId = new IdConverter().toObjectID(pnfsId.toIdString());
                        movedContainer.setObjectID(objectId);
                        _log.trace("DcacheContainerDao<Move>, setObjectID={}", objectId);

                        movedContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
                        if (containerRequest.getDomainURI() == null) {
                            movedContainer.setDomainURI("/cdmi_domains/default_domain");
                        } else {
                            movedContainer.setDomainURI(containerRequest.getDomainURI());
                        }
                        movedContainer.setMetadata("cdmi_ctime", sdf.format(now));
                        movedContainer.setMetadata("cdmi_atime", sdf.format(now));
                        movedContainer.setMetadata("cdmi_mtime", sdf.format(now));

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
                            movedContainer.setSubMetadata_ACL(subMetadata_ACL);
                        } else {
                            ArrayList<HashMap<String, String>> subMetadata_ACL = new ArrayList<HashMap<String, String>>();
                            HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                            subMetadataEntry_ACL.put("acetype", "ALLOW");
                            subMetadataEntry_ACL.put("identifier", "OWNER@");
                            subMetadataEntry_ACL.put("aceflags", "OBJECT_INHERIT, CONTAINER_INHERIT");
                            subMetadataEntry_ACL.put("acemask", "ALL_PERMS");
                            subMetadata_ACL.add(subMetadataEntry_ACL);
                            movedContainer.setSubMetadata_ACL(subMetadata_ACL);
                        }

                        // update meta information
                        try {
                            FileAttributes attr = new FileAttributes();
                            Date ctime = sdf.parse(movedContainer.getMetadata().get("cdmi_ctime"));
                            Date atime = sdf.parse(movedContainer.getMetadata().get("cdmi_atime"));
                            Date mtime = sdf.parse(movedContainer.getMetadata().get("cdmi_mtime"));
                            long ctimeAsLong = ctime.getTime();
                            long atimeAsLong = atime.getTime();
                            long mtimeAsLong = mtime.getTime();
                            attr.setCreationTime(ctimeAsLong);
                            attr.setAccessTime(atimeAsLong);
                            attr.setModificationTime(mtimeAsLong);
                            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
                            pnfs.setFileAttributes(pnfsId, attr);
                        } catch (CacheException | ParseException ex) {
                            _log.error("DcacheContainerDao<Move>, Cannot update meta information for object with objectID {}", movedContainer.getObjectID());
                        }
                        movedContainer.setCompletionStatus("Complete");
                        return completeContainer(subject, movedContainer, directory, path);

                    } else {
                            throw new BadRequestException("Error while moving container '" + path + "', no PnfsId set.");
                    }
                } else {
                    throw new BadRequestException("Error while moving container '" + path + "', no attributes available.");
                }
            }
        } else {
            throw new BadRequestException("No path given.");
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
        System.out.println("Delete container/object, path=" + path);
        if (path != null) {
            if (path.startsWith("/cdmi_objectid/") || path.startsWith("cdmi_objectid/")) {
                String newPath = "";
                if (path.startsWith("/cdmi_objectid/")) {
                    newPath = path.replace("/cdmi_objectid/", "");
                } else {
                    newPath = path.replace("cdmi_objectid/", "");
                }
                deleteByObjectId(newPath);
            } else {
                File directoryOrFile;
                directoryOrFile = absoluteFile(path);

                _log.trace("Delete container/object, path={}", directoryOrFile.getAbsolutePath());

                Subject subject = getSubject();
                if ((subject == null) || Subjects.isNobody(subject)) {
                    throw new ForbiddenException("Permission denied");
                }

                if (!isUserAllowed(subject, directoryOrFile.getAbsolutePath())) {
                    throw new ForbiddenException("Permission denied");
                }

                PnfsId pnfsId = null;
                FileAttributes attributes = getAttributes(subject, directoryOrFile.getAbsolutePath());
                if (attributes != null) {
                    pnfsId = attributes.getPnfsId();
                    if (pnfsId != null) {
                        if (checkIfDirectoryExists(subject, directoryOrFile.getAbsolutePath())) {
                            deleteDirectory(subject, directoryOrFile.getAbsolutePath(), false);
                        } else {
                            deleteFile(subject, pnfsId, directoryOrFile.getAbsolutePath());
                        }
                    } else {
                        throw new ServerErrorException("DcacheContainerDao<Delete>, Cannot retrieve metadata for object '" +  directoryOrFile.getAbsolutePath() + "'");
                    }
                } else {
                    throw new ServerErrorException("DcacheContainerDao<Delete>, Cannot retrieve metadata for object '" +  directoryOrFile.getAbsolutePath() + "'");
                }
            }
        } else {
            throw new BadRequestException("No path given.");
        }
    }

    private void deleteByObjectId(String path)
    {
        if (path != null) {
            System.out.println("Delete container/object, path=" + path);
            _log.trace("Delete container/object, path={}", path);

            String checkPath = "";
            String objectId = "";
            String restPath = "";
            Subject subject = getSubject();
            IdConverter idc = new IdConverter();
            int slashIndex = path.indexOf("/");
            if (slashIndex > 0) {
                objectId = path.substring(0, slashIndex);
                restPath = path.substring(slashIndex + 1);
                String strPnfsId = idc.toPnfsID(objectId);
                System.out.println("0005:" + strPnfsId);
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
                objectId = path;
                String strPnfsId = idc.toPnfsID(objectId);
                System.out.println("0006:" + strPnfsId);
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
            System.out.println("CheckPath: " + checkPath);

            if (checkPath != null) {
                File directoryOrFile;
                directoryOrFile = absoluteFile(checkPath);
                if ((subject == null) || Subjects.isNobody(subject)) {
                    throw new ForbiddenException("Permission denied");
                }

                if (!isUserAllowed(subject, directoryOrFile.getAbsolutePath())) {
                    throw new ForbiddenException("Permission denied");
                }

                PnfsId pnfsId = null;
                FileAttributes attributes = getAttributes(subject, directoryOrFile.getAbsolutePath());
                if (attributes != null) {
                    pnfsId = attributes.getPnfsId();
                    if (pnfsId != null) {
                        if (checkIfDirectoryExists(subject, directoryOrFile.getAbsolutePath())) {
                            deleteDirectory(subject, directoryOrFile.getAbsolutePath(), true);
                        } else {
                            deleteFile(subject, pnfsId, directoryOrFile.getAbsolutePath());
                        }
                    } else {
                        throw new ServerErrorException("DcacheContainerDao<Delete>, Cannot retrieve metadata for object '" +  directoryOrFile.getAbsolutePath() + "'");
                    }
                } else {
                    throw new ServerErrorException("DcacheContainerDao<Delete>, Cannot retrieve metadata for object '" +  directoryOrFile.getAbsolutePath() + "'");
                }
            }
        } else {
            throw new BadRequestException("No path given.");
        }
    }

    @Override
    public DcacheContainer findByPath(String path)
    {
        System.out.println("In DcacheContainerDAO.findByPath, Path=" + path);
        _log.trace("In DcacheContainerDAO.findByPath, Path={}", path);

        File directory;
        Subject subject = getSubject();
        directory = absoluteFile(path);
        DcacheContainer requestedContainer = new DcacheContainer();
        if (path != null) {
            if (!isAnonymousListingAllowed && (subject == null) && Subjects.isNobody(subject)) {
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
            directory = absoluteFile(checkPath);

            if (!checkIfDirectoryFileExists(subject, directory.getAbsolutePath())) {
//                throw new NotFoundException("Path '" + directory.getAbsolutePath()
//                        + "' does not identify an existing container");
                return null;
            }
            if (!checkIfDirectoryExists(subject, directory.getAbsolutePath())) {
                throw new BadRequestException("Path '" + directory.getAbsolutePath()
                        + "' does not identify a container");
            }
            if (!isUserAllowed(subject, directory.getAbsolutePath())) {
                throw new ForbiddenException("Permission denied");
            }

            // Setup ISO-8601 Date
            Date now = new Date();
            long nowAsLong = now.getTime();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            FileAttributes attributes = getAttributes(subject, directory.getAbsolutePath());

            PnfsId pnfsId = null;
            String objectID = "";
            int cowner = 0;
            ACL cacl = null;
            if (attributes != null) {
                pnfsId = attributes.getPnfsId();
                if (pnfsId != null) {
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
                            AceConverter ac = new AceConverter();
                            HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                            subMetadataEntry_ACL.put("acetype", ac.convertToCdmiAceType(ace.getType().name()));
                            subMetadataEntry_ACL.put("identifier", ac.convertToCdmiAceWho(ace.getWho().name()));
                            subMetadataEntry_ACL.put("aceflags", ac.convertToCdmiAceFlags(AceFlags.asString(ace.getFlags())));
                            subMetadataEntry_ACL.put("acemask", ac.convertToCdmiAceMask(AccessMask.asString(ace.getAccessMsk())));
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
                        throw new ServerErrorException("DcacheContainerDao<Read>, Cannot update meta information for object with objectID '" + requestedContainer.getObjectID() + "'");
                    }
                } else {
                    throw new ServerErrorException("DcacheContainerDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
                }
                return completeContainer(subject, requestedContainer, directory, checkPath);
            } else {
                throw new ServerErrorException("DcacheContainerDao<Read>, Cannot read meta information from directory '" + directory.getAbsolutePath() + "'");
            }
        } else {
            // root container
            requestedContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
            requestedContainer.setDomainURI("/cdmi_domains/default_domain");
            return completeContainer(subject, requestedContainer, directory, path);
        }
    }

    @Override
    public DcacheContainer findByObjectId(String objectId)
    {
        System.out.println("In DcacheContainerDAO.findByObjectId, ObjectID=" + removeSlashesFromPath(objectId));
        _log.trace("In DcacheContainerDAO.findByObjectId, ObjectID={}", removeSlashesFromPath(objectId));

        DcacheContainer requestedContainer = new DcacheContainer();
        Subject subject = getSubject();
        IdConverter idc = new IdConverter();
        String strPnfsId = idc.toPnfsID(removeSlashesFromPath(objectId));

        if (strPnfsId != null && !strPnfsId.isEmpty()) {
            PnfsId pnfsId = new PnfsId(strPnfsId);

            if (!isAnonymousListingAllowed && (subject == null) && Subjects.isNobody(subject)) {
                throw new ForbiddenException("Permission denied");
            }

            if (!isUserAllowed(subject, pnfsId)) {
                throw new ForbiddenException("Permission denied");
            }

            if (!checkIfDirectoryFileExists(subject, pnfsId)) {
                throw new NotFoundException("Object with ObjectId '" + removeSlashesFromPath(objectId)
                        + "' does not identify an existing container");
            }
            if (!checkIfDirectoryExists(subject, pnfsId)) {
                throw new BadRequestException("Object with ObjectId '" + removeSlashesFromPath(objectId)
                        + "' does not identify a container");
            }

            // Setup ISO-8601 Date
            Date now = new Date();
            long nowAsLong = now.getTime();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            FileAttributes attributes = getAttributes(subject, pnfsId);

            String path = "";
            String objectID = "";
            int cowner = 0;
            ACL cacl = null;
            if (attributes != null) {
                pnfsId = attributes.getPnfsId();
                if (pnfsId != null) {
                    path = getPnfsPath(subject, pnfsId).toString();
                    System.out.println("Path=" + path);
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
                            AceConverter ac = new AceConverter();
                            HashMap<String, String> subMetadataEntry_ACL = new HashMap<String, String>();
                            subMetadataEntry_ACL.put("acetype", ac.convertToCdmiAceType(ace.getType().name()));
                            subMetadataEntry_ACL.put("identifier", ac.convertToCdmiAceWho(ace.getWho().name()));
                            subMetadataEntry_ACL.put("aceflags", ac.convertToCdmiAceFlags(AceFlags.asString(ace.getFlags())));
                            subMetadataEntry_ACL.put("acemask", ac.convertToCdmiAceMask(AccessMask.asString(ace.getAccessMsk())));
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
                        throw new ServerErrorException("DcacheContainerDao<Read>, Cannot update meta information for object with objectID '" + requestedContainer.getObjectID() + "'");
                    }
                    return completeContainer(subject, requestedContainer, removeSlashesFromPath(objectId));
                } else {
                    throw new ServerErrorException("DcacheContainerDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
                }
            } else {
                throw new ServerErrorException("DcacheContainerDao<Read>, Cannot read meta information from directory with ObjectID '" + removeSlashesFromPath(objectId) + "'");
            }
        } else {
            throw new BadRequestException("DcacheContainerDao<Read>, No path given");
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
    private File baseDirectory()
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
     * @exception BadRequestException
     *                if the specified path identifies a data object instead of a container
     */
    private DcacheContainer completeContainer(Subject subject, DcacheContainer container, File directory, String path)
    {
        _log.trace("In DcacheContainerDao.Container, Path={}", path);
        _log.trace("In DcacheContainerDao.Container, AbsolutePath={}", directory.getAbsolutePath());
        System.out.println("In DcacheContainerDao.Container, Path=" + path);
        System.out.println("In DcacheContainerDao.Container, AbsolutePath=" + directory.getAbsolutePath());
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
        System.out.println("In DcacheContainerDao.Container, ParentURI=" + parentURI);
        String parent = "/" + removeSlashesFromPath(baseDirectoryName) + "/" + removeSlashesFromPath(parentURI);
        System.out.println("In DcacheContainerDao.Container, Parent=" + parent);
        PnfsId parentPnfsId = getPnfsIDByPath(subject, parent);
        System.out.println("In DcacheContainerDao.Container, ParentPnfsId=" + parentPnfsId.toIdString());
        String parentObjectId = new IdConverter().toObjectID(parentPnfsId.toIdString());
        container.setParentID(parentObjectId);

        // Add children containers and/or objects representing subdirectories or files
        List<String> children = container.getChildren();

        if (isAnonymousListingAllowed || ((subject != null) && !Subjects.isNobody(subject))) {
            for (Map.Entry<String, FileAttributes> entry : listDirectoriesFilesByPath(subject, directory.getAbsolutePath()).entrySet()) {
                if (entry.getValue().getFileType() == DIR && entry.getValue().getOwner() == Subjects.getUid(subject)) {
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
     * @exception BadRequestException
     *                if the specified path identifies a data object instead of a container
     */
    private DcacheContainer completeContainer(Subject subject, DcacheContainer container, String objectId)
    {
        IdConverter idc = new IdConverter();
        String strPnfsId = idc.toPnfsID(objectId);
        PnfsId pnfsId = new PnfsId(strPnfsId);
        _log.trace("In DcacheContainerDao.Container, ObjectID={}", objectId);
        _log.trace("In DcacheContainerDao.Container, PnfsID={}", pnfsId.toIdString());

        FileAttributes attributes = getAttributes(subject, pnfsId);

        String path = "";
        if (attributes != null) {
            pnfsId = attributes.getPnfsId();
            if (pnfsId != null) {
                path = getPnfsPath(subject, pnfsId).toString();
                System.out.println("Path_2=" + path);
                _log.trace("In DcacheContainerDao.Container, Path={}", path);
                container.setObjectType("application/cdmi-container");

                // Derive ParentURI
                String parentURI = "/";
                if (path != null) {
                    if (path.contains("/" + removeSlashesFromPath(baseDirectoryName))) {
                        path = path.replace("/" + removeSlashesFromPath(baseDirectoryName), "");
                    }
                    if (path.equals("/")) {
                        path = "";
                    }
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
                    for (Map.Entry<String, FileAttributes> entry : listDirectoriesFilesByPath(subject, absoluteFile(path).toString()).entrySet()) {
                        if ((entry.getValue().getFileType() == DIR) && (entry.getValue().getOwner() == Subjects.getUid(subject))) {
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
            } else {
                throw new ServerErrorException("DcacheContainerDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
            }
        } else {
            throw new ServerErrorException("DcacheContainerDao<Read>, Cannot read meta information from directory with ObjectID '" + objectId + "'");
        }
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
        String checkPath = path;
        Subject subject = getSubject();
        if (checkPath != null) {
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
                    System.out.println("0001:" + strPnfsId);
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
                    System.out.println("0002:" + strPnfsId);
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
            System.out.println("CheckPath: " + checkPath);
        }
        return (checkPath == null || isDirectory(subject, checkPath));
    }

    /**
     * DCache related stuff.
     */
    @Override
    public void afterStart()
    {
        System.out.println("CDMI service is started");
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
    private boolean isUserAllowed(Subject subject, String path)
    {
        boolean result = false;
        try {
            String tmpPath = addPrefixSlashToPath(path);
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            FileAttributes attributes = pnfs.getFileAttributes(tmpPath, REQUIRED_ATTRIBUTES);
            if (Subjects.getUid(subject) == attributes.getOwner()) {
                result = true;
            }
        } catch (CacheException ignore) {
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
        } catch (CacheException ignore) {
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
     * Returns true if object is a directory.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean isDirectory(Subject subject, String dirPath)
    {
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        System.out.println("Check: " + (baseDirectoryName + tmpDirPath));
        return checkIfDirectoryExists(subject, baseDirectoryName + tmpDirPath);
    }

    /**
     * <p>
     * Checks if a Directory exists in a specific path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean checkIfDirectoryExists(Subject subject, String dirPath)
    {
        boolean result = false;
        try {
            String tmpDirPath = addPrefixSlashToPath(dirPath);
            FileAttributes attributes = null;
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            attributes = pnfs.getFileAttributes(new FsPath(tmpDirPath), REQUIRED_ATTRIBUTES);
            if (attributes != null) {
                if (attributes.getFileType() == DIR) {
                    result = true;
                }
            }
            return result;
        } catch (CacheException ignore) {
        }
        return result;
    }

    /**
     * <p>
     * Checks if a Directory exists in a specific path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean checkIfDirectoryExists(Subject subject, PnfsId pnfsid)
    {
        boolean result = false;
        try {
            FileAttributes attributes = null;
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            attributes = pnfs.getFileAttributes(pnfsid, REQUIRED_ATTRIBUTES);
            if (attributes != null) {
                if (attributes.getFileType() == DIR) {
                    result = true;
                }
            }
            return result;
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
    private boolean checkIfDirectoryFileExists(Subject subject, String dirPath)
    {
        boolean result = false;
        try {
            String tmpDirPath = addPrefixSlashToPath(dirPath);
            FileAttributes attributes = null;
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            attributes = pnfs.getFileAttributes(new FsPath(tmpDirPath), REQUIRED_ATTRIBUTES);
            if (attributes != null) {
                result = true;
            }
            return result;
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
    private boolean checkIfDirectoryFileExists(Subject subject, PnfsId pnfsid)
    {
        boolean result = false;
        try {
            FileAttributes attributes = null;
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            attributes = pnfs.getFileAttributes(pnfsid, REQUIRED_ATTRIBUTES);
            if (attributes != null) {
                result = true;
            }
            return result;
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
        } catch (CacheException ex) {
            _log.error("DcacheDataObjectDao<getAttributes>, Could not retreive attributes for PnfsId {}", pnfsid);
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
    private Map<String, FileAttributes> listDirectoriesFilesByPath(Subject subject, String path)
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
            reply.getPnfsPath();
            result = pnfs.getFileAttributes(reply.getPnfsId(), REQUIRED_ATTRIBUTES);
        } catch (CacheException ex) {
            _log.warn("Directory '{}' could not get created, {}", dirPath, ex.getMessage());
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
     * Creates a Directory in a specific file path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private FileAttributes renameDirectory(Subject subject, String fromPath, String toPath)
    {
        FileAttributes result = null;
        String tmpFromPath = addPrefixSlashToPath(fromPath);
        String tmpToPath = addPrefixSlashToPath(toPath);
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            pnfs.renameEntry(tmpFromPath, tmpToPath, false);
            result = pnfs.getFileAttributes(tmpToPath, REQUIRED_ATTRIBUTES);
        } catch (CacheException ex) {
            _log.warn("Directory '{}' could not get renamed to '{}', {}", fromPath, toPath, ex.getMessage());
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
    private FileAttributes renameDirectory(Subject subject, PnfsId pnfsid, String toPath)
    {
        FileAttributes result = null;
        String tmpToPath = addPrefixSlashToPath(toPath);
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            pnfs.renameEntry(pnfsid, tmpToPath, false);
            result = pnfs.getFileAttributes(tmpToPath, REQUIRED_ATTRIBUTES);
        } catch (CacheException ex) {
            _log.warn("Directory with PnfsId '{}' could not get renamed to '{}', {}", pnfsid, toPath, ex.getMessage());
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
    private void deleteDirectory(Subject subject, String dirPath, boolean recursive)
    {
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        if ((dirPath == null) || dirPath.isEmpty()) {
            throw new UnauthorizedException("Permission denied");
        }
        removeDirectory(subject, tmpDirPath, recursive);
    }

    /**
     * <p>
     * Deletes a Directory in a specific file path. Needed by function deleteRecursively.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean deleteDirectory(Subject subject, PnfsId pnfsid, String dirPath)
    {
        boolean result = false;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        if (dirPath.isEmpty()) {
            throw new UnauthorizedException("Permission denied");
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

    private void removeDirectory(Subject subject, String path, boolean recursive)
    {
        if (path.isEmpty()) {
            throw new ForbiddenException("Permission denied");
        }
        String tmpPath = addPrefixSlashToPath(path);

        try {
            if (recursive) {
                removeSubdirectories(subject, new FsPath(tmpPath));
            }
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
            pnfs.deletePnfsEntry(path.toString(), EnumSet.of(DIR));
        } catch (TimeoutCacheException e) {
            throw new ServerErrorException("Name space timeout");
        } catch (FileNotFoundCacheException | NotInTrashCacheException ignored) {
            throw new NotFoundException("No such file or directory");
        } catch (NotDirCacheException e) {
            throw new NotFoundException("Not a directory");
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException("Permission denied");
        } catch (CacheException e) {
            try {
                int count = listDirectoryHandler.printDirectory(subject, new NullListPrinter(), new FsPath(tmpPath), null, Range.<Integer>all());
                if (count > 0) {
                    throw new MethodNotAllowedException("Directory is not empty", e);
                }
            } catch (InterruptedException | CacheException suppressed) {
                e.addSuppressed(suppressed);
            }
            _log.error("Failed to delete {}: {}", path, e.getMessage());
            throw new ServerErrorException("Name space failure (" + e.getMessage() + ")", e);
        }
    }

    private void removeSubdirectories(Subject subject, FsPath path)
    {
        PnfsHandler pnfs = new PnfsHandler(pnfsHandler, subject);
        FileAttributes attributes;
        try {
            attributes = pnfs.getFileAttributes(path.toString(), EnumSet.of(TYPE));
        } catch (TimeoutCacheException e) {
            throw new ServerErrorException("Name space timeout", e);
        } catch (PermissionDeniedCacheException e) {
            throw new ForbiddenException("Permission denied");
        } catch (FileNotFoundCacheException | NotInTrashCacheException e) {
            throw new NotFoundException("No such file or directory", e);
        } catch (CacheException e) {
            throw new ServerErrorException("Name space failure (" + e.getMessage() + ")");
        }
        if (attributes.getFileType() != DIR) {
            throw new NotFoundException("Not a directory");
        }
        if (path.getParent().toString().equals(removeSlashesFromPath(baseDirectoryName))) {
            if (!isUserAllowed(subject, path.getParent().toString())) {
                throw new ForbiddenException("Permission denied");
            }
        }
        List<FsPath> directories = new ArrayList<>();
        listSubdirectoriesRecursivelyForDelete(subject, path, attributes, directories);

        for (FsPath directory: directories) {
            try {
                pnfs.deletePnfsEntry(directory.toString(), EnumSet.of(DIR));
            } catch (TimeoutCacheException e) {
                throw new ServerErrorException("Name space timeout", e);
            } catch (FileNotFoundCacheException | NotInTrashCacheException ignored) {
                // Somebody removed the directory before we could.
            } catch (PermissionDeniedCacheException | NotDirCacheException e) {
                // Only directories are included in the list output, and we checked that we
                // have permission to delete them.
                throw new ServerErrorException(directory + " (directory tree was modified concurrently)");
            } catch (CacheException e) {
                // Could be because the directory is no longer empty (concurrent modification),
                // but could also be some other error.
                _log.error("Failed to delete {}: {}", directory, e.getMessage());
                throw new ServerErrorException(directory + " (" + e.getMessage() + ")");
            }
        }
    }

    /**
     * Adds transitive subdirectories of {@code dir} to {@code result}.
     *
     * @param subject Issuer of rmdir
     * @param dir Path to directory
     * @param attributes File attributes of {@code dir}
     * @param result List that subdirectories are added to
     */
    private void listSubdirectoriesRecursivelyForDelete(Subject subject, FsPath dir, FileAttributes attributes,
                                                        List<FsPath> result)
    {
        List<DirectoryEntry> children = new ArrayList<>();
        try (DirectoryStream list = listDirectoryHandler.list(subject, dir, null, Range.<Integer>all(), EnumSet.of(TYPE))) {
            for (DirectoryEntry child: list) {
                FileAttributes childAttributes = child.getFileAttributes();
                if (childAttributes.getFileType() != DIR) {
                    throw new ServerErrorException(dir + "/" + child.getName() + " (not empty)");
                }
                if (!isUserAllowed(subject, dir + "/" + child.getName())) {
                    throw new ForbiddenException("Permission denied");
                }
                children.add(child);
            }
        } catch (NotDirCacheException e) {
            throw new NotFoundException(dir + " (not a directory)", e);
        } catch (FileNotFoundCacheException | NotInTrashCacheException ignored) {
            // Somebody removed the directory before we could.
        } catch (PermissionDeniedCacheException e) {
            throw new UnauthorizedException(dir + " (permission denied)", e);
        } catch (InterruptedException e) {
            throw new ServerErrorException("Operation interrupted", e);
        } catch (TimeoutCacheException e) {
            throw new ServerErrorException("Name space timeout", e);
        } catch (CacheException e) {
            throw new ServerErrorException(dir + " (" + e.getMessage() + ")");
        }

        // Result list uses post-order so directories will be deleted bottom-up.
        for (DirectoryEntry child : children) {
            FsPath path = new FsPath(dir, child.getName());
            listSubdirectoriesRecursivelyForDelete(subject, path, child.getFileAttributes(), result);
            result.add(path);
        }
    }

    private class NullListPrinter implements DirectoryListPrinter
    {
        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return Collections.emptySet();
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) throws InterruptedException
        {
        }
    }
}