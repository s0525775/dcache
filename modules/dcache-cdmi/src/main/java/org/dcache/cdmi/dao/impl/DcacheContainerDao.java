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
import org.dcache.auth.Subjects;
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
import java.text.ParseException;
import org.dcache.acl.ACE;
import org.dcache.acl.ACL;
import org.dcache.cdmi.model.DcacheContainer;
import org.dcache.cdmi.util.IdConverter;

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
    private boolean recreate = true;

    // Properties and Dependency Injection Methods by dCache
    private CellStub pnfsStub;
    private PnfsHandler pnfsHandler;
    private ListDirectoryHandler listDirectoryHandler;
    private PnfsId pnfsId;

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
     * Set the "recreate on first use" flag that (if set) will cause any previous contents of the
     * base directory to be erased on first access. Default value for this flag is
     * <code>false</code>.
     * </p>
     *
     * @param recreate
     *            The new recreate flag value
     */
    public void setRecreate(boolean recreate)
    {
        this.recreate = recreate;
    }

    //
    // ContainerDao Methods invoked from PathResource
    //
    @Override
    public DcacheContainer createByPath(String path, Container containerRequest)
    {

        //
        // The User metadata and exports have already been de-serialized into the
        // passed Container in PathResource.putContainer()
        //

        File directory = absoluteFile(path);

        _log.trace("Create container, path={}", directory.getAbsolutePath());

        //
        // Setup ISO-8601 Date
        //
        Date now = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        DcacheContainer newContainer = (DcacheContainer) containerRequest;

        if (newContainer.getMove() == null) { // This is a normal Create or Update

            //
            // Underlying Directory existence determines whether this is a Create or
            // Update.
            //

            if (!checkIfDirectoryFileExists(directory.getAbsolutePath())) { // Creating Container

                _log.trace("<Container Create>");

                if (!createDirectory(directory.getAbsolutePath())) {
                    throw new IllegalArgumentException("Cannot create container '" + path + "'");
                }

                String objectID = "";
                int cowner = 0;
                ACL cacl = null;
                FileAttributes attr = getAttributesByPath(directory.getAbsolutePath());
                if (attr != null) {
                    pnfsId = attr.getPnfsId();
                    if (pnfsId != null) {
                        // update with real info
                        _log.trace("DcacheContainerDao<Create>, setPnfsID={}", pnfsId.toIdString());
                        newContainer.setPnfsID(pnfsId.toIdString());
                        newContainer.setMetadata("cdmi_ctime", sdf.format(attr.getCreationTime()));
                        newContainer.setMetadata("cdmi_atime", sdf.format(attr.getAccessTime()));
                        newContainer.setMetadata("cdmi_mtime", sdf.format(attr.getModificationTime()));
                        newContainer.setMetadata("cdmi_size", String.valueOf(attr.getSize()));
                        cowner = attr.getOwner();
                        cacl = attr.getAcl();
                        objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                        newContainer.setObjectID(objectID);
                        _log.trace("DcacheContainerDao<Create>, setObjectID={}", objectID);
                    } else {
                        _log.error("DcacheContainerDao<Create>, Cannot read PnfsId from meta information, ObjectID will be empty");
                    }
                } else {
                    _log.error("DcacheContainerDao<Create>, Cannot read meta information from directory {}", directory.getAbsolutePath());
                }

                //
                // TODO: Use Parent capabiltiesURI if not specified in create body
                //

                newContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");

                //
                // TODO: Use Parent Domain if not specified in create body
                //
                if (newContainer.getDomainURI() == null) {
                    newContainer.setDomainURI("/cdmi_domains/default_domain");
                }

                newContainer.setMetadata("cdmi_ctime", sdf.format(now));
                newContainer.setMetadata("cdmi_acount", "0");
                newContainer.setMetadata("cdmi_mcount", "0");
                newContainer.setMetadata("cdmi_owner", Subjects.ROOT.toString());
                newContainer.setMetadata("cdmi_owner", String.valueOf(cowner));  //TODO
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

            } else { // Updating Container

                _log.trace("<Container Update>");

                DcacheContainer currentContainer = new DcacheContainer();

                String objectID = "";
                int cowner = 0;
                ACL cacl = null;
                FileAttributes attr = getAttributesByPath(directory.getAbsolutePath());
                if (attr != null) {
                    pnfsId = attr.getPnfsId();
                    if (pnfsId != null) {
                        // update with real info
                        _log.trace("DcacheContainerDao<Update>, setPnfsID={}", pnfsId.toIdString());
                        currentContainer.setPnfsID(pnfsId.toIdString());
                        currentContainer.setMetadata("cdmi_ctime", sdf.format(attr.getCreationTime()));
                        currentContainer.setMetadata("cdmi_atime", sdf.format(attr.getAccessTime()));
                        currentContainer.setMetadata("cdmi_mtime", sdf.format(attr.getModificationTime()));
                        currentContainer.setMetadata("cdmi_size", String.valueOf(attr.getSize()));
                        cowner = attr.getOwner();
                        cacl = attr.getAcl();
                        objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                        currentContainer.setObjectID(objectID);
                        _log.trace("DcacheContainerDao<Update>, setObjectID={}", objectID);
                    } else {
                        _log.error("DcacheContainerDao<Update>, Cannot read PnfsId from meta information, ObjectID will be empty");
                    }
                } else {
                    _log.error("DcacheContainerDao<Update>, Cannot read meta information from directory {}", directory.getAbsolutePath());
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

                newContainer.setPnfsID(pnfsId.toIdString());
                newContainer.setObjectID(objectID);

                //
                // TODO: Need to handle update of Capabilities URI
                //
                newContainer.setCapabilitiesURI(currentContainer.getCapabilitiesURI());

                //
                // TODO: Need to handle update of Domain
                //
                newContainer.setDomainURI(currentContainer.getDomainURI());

                //forth-and-back update
                for (String key : newContainer.getMetadata().keySet()) {
                    currentContainer.setMetadata(key, newContainer.getMetadata().get(key));
                }
                for (String key : currentContainer.getMetadata().keySet()) {
                    newContainer.setMetadata(key, currentContainer.getMetadata().get(key));
                }
                newContainer.setMetadata("cdmi_mtime", sdf.format(now));

                //
                // TODO: Need to handle update of ACL info
                //
                newContainer.setSubMetadata_ACL(currentContainer.getSubMetadata_ACL());  //doesn't really work
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
                PnfsId id = new PnfsId(newContainer.getPnfsID());
                pnfsHandler.setFileAttributes(id, attr);
            } catch (CacheException | ParseException ex) {
                _log.error("DcacheContainerDao<Update>, Cannot update meta information for object with objectID {}", newContainer.getObjectID());
            }

            //
            // Transient fields
            //
            containerRequest.setCompletionStatus("Complete");

            //
            // Complete response with fields dynamically generated from directory info.
            //

            return completeContainer(newContainer, directory, path);

        } else { // Moving a Container

            //TODO: This part is still in process.

            DcacheContainer movedContainer = new DcacheContainer();

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
    public void deleteByPath(String path)
    {
        File directoryOrFile = absoluteFile(path);

        _log.trace("Delete container/object, path={}", directoryOrFile.getAbsolutePath());

        //
        // Setup ISO-8601 Date
        //
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        //
        String objectID = "";
        DcacheContainer requestedContainer = new DcacheContainer();
        FileAttributes attr = getAttributesByPath(directoryOrFile.getAbsolutePath());
        if (attr != null) {
            pnfsId = attr.getPnfsId();
            if (pnfsId != null) {
                // update with real info
                _log.trace("DcacheContainerDao<Delete>, setPnfsID={}", pnfsId.toIdString());
                requestedContainer.setPnfsID(pnfsId.toIdString());
                requestedContainer.setMetadata("cdmi_ctime", sdf.format(attr.getCreationTime()));
                requestedContainer.setMetadata("cdmi_atime", sdf.format(attr.getAccessTime()));
                requestedContainer.setMetadata("cdmi_mtime", sdf.format(attr.getModificationTime()));
                requestedContainer.setMetadata("cdmi_size", String.valueOf(attr.getSize()));
                objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                requestedContainer.setObjectID(objectID);
                _log.trace("DcacheContainerDao<Delete>, setObjectID={}", objectID);
            } else {
                _log.error("DcacheContainerDao<Delete>, Cannot read PnfsId from meta information, ObjectID will be empty");
            }
        } else {
            _log.error("DcacheContainerDao<Delete>, Cannot read meta information from directory or object {}", directoryOrFile.getAbsolutePath());
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

    @Override
    public DcacheContainer findByObjectId(String objectId)
    {
        throw new UnsupportedOperationException("DcacheContainerDao.findByObjectId()");
    }

    @Override
    public DcacheContainer findByPath(String path)
    {
        _log.trace("In DcacheContainerDAO.findByPath, Path={}", path);

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

        // Setup ISO-8601 Date
        Date now = new Date();
        long nowAsLong = now.getTime();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        DcacheContainer requestedContainer = new DcacheContainer();

        if (path != null) {

            String objectID = "";
            int cowner = 0;
            ACL cacl = null;
            FileAttributes attr = getAttributesByPath(directory.getAbsolutePath());
            if (attr != null) {
                pnfsId = attr.getPnfsId();
                if (pnfsId != null) {
                    // update with real info
                    _log.trace("DcacheContainerDao<Read>, setPnfsID={}", pnfsId.toIdString());
                    requestedContainer.setPnfsID(pnfsId.toIdString());
                    requestedContainer.setMetadata("cdmi_ctime", sdf.format(attr.getCreationTime()));
                    requestedContainer.setMetadata("cdmi_atime", sdf.format(attr.getAccessTime()));
                    requestedContainer.setMetadata("cdmi_mtime", sdf.format(attr.getModificationTime()));
                    requestedContainer.setMetadata("cdmi_size", String.valueOf(attr.getSize()));
                    cowner = attr.getOwner();
                    cacl = attr.getAcl();
                    objectID = new IdConverter().toObjectID(pnfsId.toIdString());
                    requestedContainer.setObjectID(objectID);
                    _log.trace("DcacheContainerDao<Read>, setObjectID={}", objectID);
                } else {
                    _log.error("DcacheContainerDao<Read>, Cannot read PnfsId from meta information, ObjectID will be empty");
                }
            } else {
                _log.error("DcacheContainerDao<Read>, Cannot read meta information from directory {}", directory.getAbsolutePath());
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

            // update meta information
            try {
                FileAttributes attr2 = new FileAttributes();
                attr2.setAccessTime(nowAsLong);
                pnfsHandler.setFileAttributes(pnfsId, attr2);
                requestedContainer.setMetadata("cdmi_atime", sdf.format(now));
            } catch (CacheException ex) {
                _log.error("DcacheContainerDao<Read>, Cannot update meta information for object with objectID {}", requestedContainer.getObjectID());
            }

        } else {

            // if this is the root container there is no "." metadata file up one level.
            // Dynamically generate the default values

            requestedContainer.setCapabilitiesURI("/cdmi_capabilities/container/default");
            requestedContainer.setDomainURI("/cdmi_domains/default_domain");
        }

        return completeContainer(requestedContainer, directory, path);
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
    public File absoluteFile(String path)
    {
        if (path == null) {
            return baseDirectory();
        } else {
            return new File(baseDirectory(), path);
        }
    }

    /**
     * <p>
     * Return a {@link File} instance for the base directory, erasing any previous content on first
     * use if the <code>recreate</code> flag has been set.
     * </p>
     *
     * @exception IllegalArgumentException
     *                if we cannot create the base directory
     */
    private File baseDirectory()
    {
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
    private DcacheContainer completeContainer(DcacheContainer container, File directory, String path)
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

        // Add children containers and/or objects representing subdirectories or
        // files

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
        //I know that NetBeans will show that I can put the whole code in one line (like in Scala), but I liked this way.
        if (path == null || isDirectory(path)) {
            return true;
        } else {
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
    private boolean isDirectory(String dirPath)
    {
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        return checkIfDirectoryExists(baseDirectoryName + tmpDirPath);
    }

    /**
     * <p>
     * Returns true if a Directory exists in a specific path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private boolean isExistingDirectory(String dirPath)
    {
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        return checkIfDirectoryExists(tmpDirPath);
    }

    /**
     * <p>
     * Checks if a Directory exists in a specific path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
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
     * Lists all Directories in a specific path.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
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
    private boolean createDirectory(String dirPath)
    {
        boolean result = false;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        try {
            pnfsHandler.createDirectories(new FsPath(tmpDirPath));
            result = true;
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
    private boolean deleteDirectory(String dirPath)
    {
        boolean result = false;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        try {
            pnfsHandler.deletePnfsEntry(tmpDirPath);
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
    private boolean deleteFile(String filePath)
    {
        boolean result = false;
        String tmpFilePath = addPrefixSlashToPath(filePath);
        try {
            pnfsHandler.deletePnfsEntry(tmpFilePath);
            result = true;
        } catch (CacheException ex) {
            _log.warn("File '{}' could not get deleted, {}", filePath, ex.getMessage());
        }
        return result;
    }

    /**
     * <p>
     * Delete the specified directory, after first recursively deleting any contents within it.
     * </p>
     *
     * @param directory
     *            {@link String} identifying the directory to be deleted
     */
    private void deleteRecursively(String path)
    {
        String tmpPath = addPrefixSlashToPath(path);
        Map<String, FileAttributes> listing = listDirectoriesFilesByPath(tmpPath);
        for (Map.Entry<String, FileAttributes> entry : listing.entrySet()) {
            if (entry.getValue().getFileType() == REGULAR) {
                deleteFile(entry.getKey());
            } else {
                deleteRecursively(entry.getKey());
            }
        }
        deleteDirectory(tmpPath);
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