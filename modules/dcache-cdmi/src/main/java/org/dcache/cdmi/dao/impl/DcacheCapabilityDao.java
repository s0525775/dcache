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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import dmg.cells.nucleus.CellLifeCycleAware;
import java.io.File;
import java.util.EnumSet;
import java.util.Set;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;
import org.dcache.cdmi.exception.ServerErrorException;
import org.dcache.cdmi.model.DcacheCapability;
import org.dcache.cdmi.util.IdConverter;
import org.dcache.cells.CellStub;
import org.dcache.namespace.FileAttribute;
import static org.dcache.namespace.FileAttribute.ACCESS_TIME;
import static org.dcache.namespace.FileAttribute.ACL;
import static org.dcache.namespace.FileAttribute.CHANGE_TIME;
import static org.dcache.namespace.FileAttribute.CREATION_TIME;
import static org.dcache.namespace.FileAttribute.MODE;
import static org.dcache.namespace.FileAttribute.MODIFICATION_TIME;
import static org.dcache.namespace.FileAttribute.OWNER;
import static org.dcache.namespace.FileAttribute.OWNER_GROUP;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.STORAGEINFO;
import static org.dcache.namespace.FileAttribute.TYPE;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.dao.CapabilityDao;
import org.snia.cdmiserver.model.Capability;
import org.snia.cdmiserver.util.ObjectID;

/* This class is dCache's DAO implementation class for SNIA's CapabilityDao interface.
   Capabilities represents the functionalities of a Cloud System of a Container and of a DataObject,
   that means which Cloud services are supported and provided if the Cloud client asks for them.
   This class contains all operations which are related to capability operations.
   It's used for the metadata communication between CDMI server and CDMI client.
   This class is already different from SNIA's CDMI reference implementation.
*/

/**
 * <p>
 * Concrete implementation of {@link CapabilityObjectDao} using the local filesystem as the backing
 * store.
 * </p>
 */
public class DcacheCapabilityDao
             implements CapabilityDao, CellLifeCycleAware
{

    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcacheCapabilityDao.class);

    // Properties and Dependency Injection Methods by CDMI
    private static final String MAIN_DIRECTORY = "cdmi_capabilities";
    private String baseDirectoryName = null;
    private File baseDirectory = null;

    // Properties and Dependency Injection Methods by dCache
    private static final Set<FileAttribute> REQUIRED_ATTRIBUTES = EnumSet.of(PNFSID);
    private CellStub pnfsStub;
    private PnfsHandler pnfsHandler;

    private final static ImmutableList<String> CAPABILITY_MAINTREE = new ImmutableList.Builder<String>()
        //.add("domain")
        .add("container")
        .add("dataobject")
        .build();

    private final static ImmutableList<String> CAPABILITY_SUBTREE = new ImmutableList.Builder<String>()
        .add("default")
        .build();

    private final static ImmutableMap<String, String> CONTAINER_METADATA = new ImmutableMap.Builder<String, String>()
        .put("cdmi_list_children", "true")
        .put("cdmi_read_metadata", "true")
        .put("cdmi_modify_metadata", "true")
        .put("cdmi_create_dataobject", "true")
        .put("cdmi_create_container", "true")
        .build();

    private final static ImmutableMap<String, String> DEFAULT_CONTAINER_METADATA = new ImmutableMap.Builder<String, String>()
        .put("cdmi_list_children", "true")
        .put("cdmi_read_metadata", "true")
        .put("cdmi_modify_metadata", "true")
        .put("cdmi_create_dataobject", "true")
        .put("cdmi_post_dataobject", "true")
        .put("cdmi_create_container", "true")
        .build();

    private final static ImmutableMap<String, String> DATAOBJECT_METADATA = new ImmutableMap.Builder<String, String>()
        .put("cdmi_read_value", "true")
        .put("cdmi_read_metadata", "true")
        .put("cdmi_modify_metadata", "true")
        .put("cdmi_modify_value", "true")
        .put("cdmi_delete_dataobject", "true")
        .build();

    private final static ImmutableMap<String, String> DEFAULT_DATAOBJECT_METADATA = new ImmutableMap.Builder<String, String>()
        .put("cdmi_read_value", "true")
        .put("cdmi_read_metadata", "true")
        .put("cdmi_modify_metadata", "true")
        .put("cdmi_modify_value", "true")
        .put("cdmi_delete_dataobject", "true")
        .build();

    private final static ImmutableMap<String, String> DEFAULT_METADATA = new ImmutableMap.Builder<String, String>()
        .put("domains", "false")
        .put("cdmi_export_occi_iscsi", "true")
        .put("cdmi_metadata_maxitems", "1024")
        .put("cdmi_metadata_maxsize", "4096")
        .put("cdmi_assignedsize", "false")
        .put("cdmi_data_redundancy", "")
        .put("cdmi_data_dispersion", "false")
        .put("cdmi_data_retention", "false")
        .put("cdmi_data_autodelete", "false")
        .put("cdmi_data_holds", "false")
        .put("cdmi_encryption", "{}")
        .put("cdmi_geographic_placement", "false")
        .put("cdmi_immediate_redundancy", "")
        .put("cdmi_infrastructure_redundancy", "")
        .put("cdmi_latency", "false")
        .put("cdmi_RPO", "false")
        .put("cdmi_RTO", "false")
        .put("cdmi_sanitization_method", "{}")
        .put("cdmi_throughput", "false")
        .put("cdmi_value_hash", "{}")
        .build();

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
        _log.trace("BaseDirectory(F)={}", baseDirectoryName);
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

    @Override
    public Capability findByObjectId(String objectId)
    {
        //is taken over by DcacheContainerDao
        throw new UnsupportedOperationException("DcacheCapabilityDao.findByObjectId()");
    }

    @Override
    public Capability findByPath(String path)
    {
        File directory;
        String objectId = "";
        DcacheCapability capability = new DcacheCapability();

        System.out.println("In DcacheCapabilityDao.findByPath, path={" + path + "}");
        _log.trace("In DcacheCapabilityDao.findByPath, path={}", path);
        switch (path) {
            case "container":
            case "container/":
                _log.trace("Container Capabilities");
                // Container Capabilities
                capability.getMetadata().putAll(CONTAINER_METADATA);
                capability.getChildren().addAll(CAPABILITY_SUBTREE);
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY) + "/container");
                objectId = getAttr(directory.getAbsolutePath());
                capability.setObjectID(objectId);
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY));
                capability.setParentURI(addPrefixSlashToPath(removeSlashesFromPath(MAIN_DIRECTORY)));
                objectId = getAttr(directory.getAbsolutePath());
                capability.setParentID(objectId);
                break;
            case "container/default":
            case "container/default/":
                _log.trace("Default Container Capabilities");
                capability.getMetadata().putAll(DEFAULT_CONTAINER_METADATA);
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY) + "/container/default");
                objectId = getAttr(directory.getAbsolutePath());
                capability.setObjectID(objectId);
                capability.setParentURI(addPrefixSlashToPath(removeSlashesFromPath(MAIN_DIRECTORY) + "/container"));
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY) + "/container");
                objectId = getAttr(directory.getAbsolutePath());
                capability.setParentID(objectId);
                break;
            case "dataobject":
            case "dataobject/":
                // Data Object Capabilities
                _log.trace("Data Object Capabilities");
                capability.getMetadata().putAll(DATAOBJECT_METADATA);
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY) + "/dataobject");
                objectId = getAttr(directory.getAbsolutePath());
                capability.setObjectID(objectId);
                capability.setParentURI(addPrefixSlashToPath(removeSlashesFromPath(MAIN_DIRECTORY)));
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY));
                objectId = getAttr(directory.getAbsolutePath());
                capability.setParentID(objectId);
                break;
            case "dataobject/default":
            case "dataobject/default/":
                _log.trace("Default Data Object Capabilities");
                capability.getMetadata().putAll(DEFAULT_DATAOBJECT_METADATA);
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY) + "/dataobject/default");
                objectId = getAttr(directory.getAbsolutePath());
                capability.setObjectID(objectId);
                capability.setParentURI(addPrefixSlashToPath(removeSlashesFromPath(MAIN_DIRECTORY) + "/dataobject"));
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY) + "/dataobject");
                objectId = getAttr(directory.getAbsolutePath());
                capability.setParentID(objectId);
                break;
            default:
                // System Capabilities
                _log.trace("System Capabilities");
                capability.getMetadata().putAll(DEFAULT_METADATA);
                capability.getChildren().addAll(CAPABILITY_MAINTREE);
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY));
                objectId = getAttr(directory.getAbsolutePath());
                capability.setObjectID(objectId);
                capability.setParentURI("/");
                capability.setParentID(objectId);
                break;
        }
        capability.setObjectType("application/cdmi-capability");
        return (DcacheCapability) capability;
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
     * Creates a Directory in a specific file path.
     * </p>
     *
     * @param dirPath
     *            {@link String} identifying a directory path
     */
    private FileAttributes createDirectory(String dirPath)
    {
        FileAttributes result = null;
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        try {
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, Subjects.ROOT);
            PnfsCreateEntryMessage reply = pnfs.createPnfsDirectory(tmpDirPath);
            reply.getPnfsPath();
            result = pnfs.getFileAttributes(reply.getPnfsId(), REQUIRED_ATTRIBUTES);
        } catch (CacheException ex) {
            _log.warn("DcacheCapabilityDao<createDirectory>, Directory '{}' could not get created, {}", dirPath, ex.getMessage());
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
        try {
            String tmpDirPath = addPrefixSlashToPath(dirPath);
            FileAttributes attributes = null;
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, Subjects.ROOT);
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
     * Lists all File Attributes for specific directory.
     * </p>
     *
     * @param path
     *            {@link String} identifying a directory path
     */
    private FileAttributes getAttributes(String path)
    {
        FileAttributes result = null;
        try {
            String tmpDirPath = addPrefixSlashToPath(path);
            PnfsHandler pnfs = new PnfsHandler(pnfsHandler, Subjects.ROOT);
            result = pnfs.getFileAttributes(new FsPath(tmpDirPath), REQUIRED_ATTRIBUTES);
        } catch (CacheException ex) {
            _log.error("DcacheCapabilityDao<getAttributes>, Could not retreive attributes for path {}", path);
        }
        return result;
    }

    private String getAttr(String path)
    {
        String result = "";
        PnfsId pnfsId = null;
        String tempPath = addPrefixSlashToPath(path);
        FileAttributes attributes = getAttributes(tempPath);
        if (attributes != null) {
            pnfsId = attributes.getPnfsId();
            if (pnfsId != null) {
                result = new IdConverter().toObjectID(pnfsId.toIdString());
            } else {
                _log.warn("DcacheCapabilityDao<geAttr>, Error while creating container '{}', no PnfsId set.", path);
            }
        } else {
            _log.warn("DcacheCapabilityDao<getAttr>, Error while creating container '{}', no attributes available.", path);
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

    private void init()
    {
        File directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY));
        if (!checkIfDirectoryFileExists(directory.getAbsolutePath())) {
            createDir(directory.getAbsolutePath());
        }
        for (String mainpath : CAPABILITY_MAINTREE) {
            directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY) + "/" + removeSlashesFromPath(mainpath));
            if (!checkIfDirectoryFileExists(directory.getAbsolutePath())) {
                createDir(directory.getAbsolutePath());
            }
            for (String subpath : CAPABILITY_SUBTREE) {
                directory = absoluteFile(removeSlashesFromPath(MAIN_DIRECTORY) + "/" + removeSlashesFromPath(mainpath)
                                         + "/" + removeSlashesFromPath(subpath));
                if (!checkIfDirectoryFileExists(directory.getAbsolutePath())) {
                    createDir(directory.getAbsolutePath());
                }
            }
        }
    }

    private void createDir(String path)
    {
        _log.trace("Create container, path={}", path);
        String tempPath = addPrefixSlashToPath(path);
        FileAttributes attributes = createDirectory(tempPath);
        PnfsId pnfsId = null;
        if (attributes != null) {
            pnfsId = attributes.getPnfsId();
            if (pnfsId == null) {
                _log.warn("DcacheCapabilityDao<createDir>, Error while creating container '{}', no PnfsId set.", path);
            }
        } else {
            _log.warn("DcacheCapabilityDao<createDir>, Error while creating container '{}', no attributes available.", path);
        }
    }

    @Override
    public void afterStart()
    {
        //Create CapabilityTree as a user which has always write rights
        //Capabilities must be available for every user
        init();
    }

    @Override
    public void beforeStop()
    {
    }

}
