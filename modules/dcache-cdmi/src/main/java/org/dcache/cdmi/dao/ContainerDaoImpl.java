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
package org.dcache.cdmi.dao;

import com.google.common.collect.Range;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
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
import org.dcache.cdmi.temp.Test;
import org.dcache.cells.CellLifeCycleAware;

import org.dcache.cdmi.mover.CDMIProtocolInfo;
import org.dcache.cells.AbstractCellComponent;
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
import org.snia.cdmiserver.util.ObjectID;

/**
 * <p>
 * Concrete implementation of {@link ContainerDao} using the local filesystem as the backing store.
 * </p>
 */
public class ContainerDaoImpl extends AbstractCellComponent
    implements ContainerDao, ServletContextListener, CellLifeCycleAware {

    //
    // Something important
    //
    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(ContainerDaoImpl.class);

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
    public static final String ATTRIBUTE_NAME_PNFSSTUB = "org.dcache.cdmi.pnfsstub";
    public static final String ATTRIBUTE_NAME_LISTER = "org.dcache.cdmi.lister";
    public static final String ATTRIBUTE_NAME_POOLSTUB = "org.dcache.cdmi.poolstub";
    public static final String ATTRIBUTE_NAME_POOLMGRSTUB = "org.dcache.cdmi.poolmgrstub";
    public static final String ATTRIBUTE_NAME_BILLINGSTUB = "org.dcache.cdmi.billingstub";

    /**
     * CDMI related stuff.
     */

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
    }

    private boolean recreate = true;

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
    public Container createByPath(String path, Container containerRequest) {

        //
        // The User metadata and exports have already been de-serialized into the
        // passed Container in PathResource.putContainer()
        //

        String directory = absolutePath(path);

        String containerFieldsFile = getContainerFieldsFile(path);

        if (containerRequest.getMove() == null) { // This is a normal Create or Update

            //
            // Setup ISO-8601 Date
            //
            Date now = new Date();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            //
            // Underlying Directory existence determines whether this is a Create or
            // Update.
            //

            if (!checkIfDirectoryFileExists(directory)) { // Creating Container

                if (!createDirectory(directory)) {
                    throw new IllegalArgumentException("Cannot create container '" + path + "'");
                }

                String objectID = ObjectID.getObjectID(9); // System.nanoTime()+"";
                containerRequest.setObjectID(objectID);

                //
                // TODO: Use Parent capabiltiesURI if not specified in create body
                //

                containerRequest.setCapabilitiesURI("/cdmi_capabilities/container/default");

                //
                // TODO: Use Parent Domain if not specified in create body
                //
                if (containerRequest.getDomainURI() == null)
                    containerRequest.setDomainURI("/cdmi_domains/default_domain");

                Map<String, Object> exports = containerRequest.getExports();
                if (exports.containsKey("OCCI/NFS")) {
                    // Export this directory (OpenSolaris only so far)
                    // Runtime runtime = Runtime.getRuntime();
                    // String exported =
                    // "pfexec share -f nfs -o rw=10.1.254.117:10.1.254.122:10.1.254.123:10.1.254.124:10.1.254.125:10.1.254.126:10.1.254.127 "
                    // + containerFieldsFile.getAbsolutePath();
                    // runtime.exec(exported);
                }

                containerRequest.getMetadata().put("cdmi_ctime", sdf.format(now));
                containerRequest.getMetadata().put("cdmi_mtime", "never");
                containerRequest.getMetadata().put("cdmi_atime", "never");
                containerRequest.getMetadata().put("cdmi_acount", "0");
                containerRequest.getMetadata().put("cdmi_mcount", "0");

            } else { // Updating Container

                //
                // Read the persistent metatdata from the "." file
                //
                //TODO:
                Container currentContainer = getPersistedContainerFields(containerFieldsFile);

                containerRequest.setObjectID(currentContainer.getObjectID());

                //
                // TODO: Need to handle update of Domain
                //

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

                containerRequest.getMetadata().put(
                        "cdmi_ctime",
                        currentContainer.getMetadata().get("cdmi_ctime"));
                containerRequest.getMetadata().put(
                        "cdmi_atime",
                        currentContainer.getMetadata().get("cdmi_atime"));
                containerRequest.getMetadata().put("cdmi_mtime", sdf.format(now));
            }

            //
            // Write created or updated persisted fields out to the "." file
            //

            try {
                FileWriter fstream = new FileWriter(absolutePath(containerFieldsFile));
                BufferedWriter out = new BufferedWriter(fstream);
                out.write(containerRequest.toJson(true)); // Save it
                out.close();
            } catch (Exception ex) {
                ex.printStackTrace();
                System.out.println("Exception while writing: " + ex);
                throw new IllegalArgumentException("Cannot write container fields file @"
                                                   + path
                                                   + " error : "
                                                   + ex);
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

            if (checkIfDirectoryFileExists(directory)) {
                throw new IllegalArgumentException("Cannot move container '"
                                                   + containerRequest.getMove()
                                                   + "' to '"
                                                   + path
                                                   + "'; Destination already exists");
            }

            String sourceContainerFile = absolutePath(containerRequest.getMove());

            if (!checkIfDirectoryFileExists(sourceContainerFile)) {
                throw new NotFoundException("Path '"
                                            + absolutePath(directory)
                                            + "' does not identify an existing container");
            }
            if (!isDirectory(sourceContainerFile)) {
                throw new IllegalArgumentException("Path '"
                                                   + absolutePath(directory)
                                                   + "' does not identify a container");
            }

            //
            // Move Container directory
            //

            renameDirectory(sourceContainerFile, directory);

            //
            // Move Container's Metadata .file
            //
            String sourceContainerFieldsFile = getContainerFieldsFile(containerRequest.getMove());

            renameFile(sourceContainerFieldsFile, containerFieldsFile);

            //
            // Get the containers field's to return in response
            //

            Container movedContainer = getPersistedContainerFields(containerFieldsFile);

            //
            // If the request has a metadata field, replace any metadata filed in the source
            // Container
            //

            if (!containerRequest.getMetadata().isEmpty()) {
                String cdmi_ctime = movedContainer.getMetadata().get("cdmi_ctime");
                String cdmi_mtime = movedContainer.getMetadata().get("cdmi_mtime");
                String cdmi_atime = movedContainer.getMetadata().get("cdmi_atime");
                String cdmi_acount = movedContainer.getMetadata().get("cdmi_acount");
                String cdmi_mcount = movedContainer.getMetadata().get("cdmi_mcount");

                movedContainer.setMetaData(containerRequest.getMetadata());

                movedContainer.getMetadata().put("cdmi_ctime", cdmi_ctime);
                movedContainer.getMetadata().put("cdmi_mtime", cdmi_mtime);
                movedContainer.getMetadata().put("cdmi_atime", cdmi_atime);
                movedContainer.getMetadata().put("cdmi_acount", cdmi_acount);
                movedContainer.getMetadata().put("cdmi_mcount", cdmi_mcount);

                //
                // Write created or updated persisted fields out to the "." file
                //

                try {
                    FileWriter fstream = new FileWriter(absolutePath(containerFieldsFile));
                    BufferedWriter out = new BufferedWriter(fstream);
                    out.write(containerRequest.toJson(true)); // Save it
                    out.close();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.out.println("Exception while writing: " + ex);
                    throw new IllegalArgumentException("Cannot write container fields file @"
                                                       + path
                                                       + " error : "
                                                       + ex);
                }

            }

            //
            // Transient fields
            //

            movedContainer.setCompletionStatus("Complete");

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
        String directoryOrFile = absolutePath(path);

        _log.error("DELETE: " + directoryOrFile);

        //

        if (isDirectory(directoryOrFile)) {
            deleteRecursively(directoryOrFile);
        } else {
            deleteFile(directoryOrFile);
        }

        //
        // remove the "." file that contains the Container or Object's JSON-encoded
        // metadata
        //
        deleteFile(getContainerFieldsFile(path));
    }

    //
    // Not Implemented
    //
    @Override
    public Container findByObjectId(String objectId) {
        throw new UnsupportedOperationException("ContainerDaoImpl.findByObjectId()");
    }

    //
    //
    //
    @Override
    public Container findByPath(String path) {

        System.out.println("In ContainerDAO.findByPath : " + path);

        String directory = absolutePath(path);

        if (directory == null) {
            directory = "/disk";
        }

        if (!checkIfDirectoryFileExists(directory)) {
            throw new NotFoundException("Path '"
                                        + absolutePath(path)
                                        + "' does not identify an existing container");
        }
        if (!isDirectory(directory)) {
            throw new IllegalArgumentException("Path '"
                                               + absolutePath(path)
                                               + "' does not identify a container");
        }

        Container requestedContainer = new Container();

        if (path != null) {

            //
            // Read the persisted container fields from the "." file
            //
            requestedContainer = getPersistedContainerFields(getContainerFieldsFile(path));

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
    private String getContainerFieldsFile(String path) {
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
        System.out.println("Path = " + path);
        System.out.println("Parent Container Name = "
                           + parentContainerName
                           + " Container Name == "
                           + containerName);


        String baseDirectory1, parentContainerDirectory, containerFieldsFile;
        try {
            System.out.println("baseDirectory = " + baseDirectoryName);
            baseDirectory1 = baseDirectoryName + "/";
            System.out
                    .println("Base Directory Absolute Path = " + absolutePath(baseDirectory1));
            parentContainerDirectory = baseDirectory1 + parentContainerName;  //CHG
            //
            System.out.println("Parent Container Absolute Path = "
                               + absolutePath(parentContainerDirectory));
            //
            containerFieldsFile = parentContainerDirectory + "/" + containerFieldsFileName;
            System.out.println("Container Metadata File Path = "
                               + absolutePath(containerFieldsFile));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Exception while building File objects: " + ex);
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
    private Container getPersistedContainerFields(String containerFieldsFile) {
        Container containerFields = new Container();
        try {
            FileInputStream in = new FileInputStream(absolutePath(containerFieldsFile));
            int inpSize = in.available();
            System.out.println("Container fields file size:" + inpSize);

            byte[] inBytes = new byte[inpSize];
            in.read(inBytes);

            containerFields.fromJson(inBytes, true);
            String mds = new String(inBytes);
            System.out.println("Container fields read were:" + mds);

            // Close the output stream
            in.close();
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("Exception while reading: " + ex);
            throw new IllegalArgumentException("Cannot read container fields file error : " + ex);
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
     */
    public String absolutePath(String path) {
        if (path == null || path.equals("null")) {
            return baseDirectory();
        } else {
            String tmpPath = addPrefixSlashToPath(path);
            return baseDirectory() + tmpPath;
        }
    }

    private String baseDirectory = null;

    /**
     * <p>
     * Return a {@link File} instance for the base directory, erasing any previous content on first
     * use if the <code>recreate</code> flag has been set.
     * </p>
     *
     * @exception IllegalArgumentException
     *                if we cannot create the base directory
     */
    private String baseDirectory() {
        if (baseDirectoryName == null) {
            if (recreate) {
                String baseDir = addPrefixSlashToPath(baseDirectoryName);
                deleteRecursively(baseDir);
                if (!createDirectory(baseDir)) {
                    throw new IllegalArgumentException("Cannot create base directory '"
                            + baseDirectoryName
                            + "'");
                }
            }
        }
        _log.error("BaseDirectory GGG:" + baseDirectory);
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
    private Container completeContainer(Container container, String directory, String path) {
        System.out.println("In ContainerDaoImpl.Container, path is: " + path);

        System.out.println("In ContainerDaoImpl.Container, absolute path is: "
                           + absolutePath(directory));


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
            System.out.println("In ContainerDaoImpl.Container, ParentURI = "
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

        for (Map.Entry<String, FileType> entry : listDirectoriesFilesByPath(directory).entrySet()) {
            if (entry.getValue() == DIR) {
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
        String directoryOrFile = absolutePath(path);
        if (directoryOrFile == null) {
            directoryOrFile = "/disk";
        }
        if (isDirectory(directoryOrFile)) {
            _log.error("ISDIR!!!");
            return true;
        } else {
            _log.error("ISNOTDIR!!!");
            return false;
        }
    }

    /**
     * DCache related stuff.
     */

    private void init() {
        pnfsStub = CDMIDataTransfer.getPnfsStub();
        pnfsHandler = CDMIDataTransfer.getPnfsHandler();
        listDirectoryHandler = CDMIDataTransfer.getListDirectoryHandler();
        poolStub = CDMIDataTransfer.getPoolStub();
        poolMgrStub = CDMIDataTransfer.getPoolMgrStub();
        billingStub = CDMIDataTransfer.getBillingStub();
    }

    //This function is necessary, otherwise the attributes and servletContext are not set.
    //It is called before afterStart() of the CellLifeCycleAware interface, which is wanted, too.
    //In other words: contextInitialized() must be called before afterStart().
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        this.servletContext = servletContextEvent.getServletContext();
        this.pnfsStub = getCellStubAttribute();
        this.pnfsHandler = new PnfsHandler(pnfsStub);
        //this.listDirectoryHandler = new ListDirectoryHandler(pnfsHandler); //does not work, tested 100 times
        this.listDirectoryHandler = getListDirAttribute(); //it only works in this way, tested 100 times
        this.poolStub = getPoolAttribute();
        this.poolMgrStub = getPoolMgrAttribute();
        this.billingStub = getBillingAttribute();
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
        //Temporary... Start...
        Test.write("/tmp/testb001.log", "0001");
        boolean test01 = createDirectory("/disk/test123");
        boolean test02 = createDirectory("/disk/test345");
        boolean test1 = createDirectory("/disk/test234");
        Test.write("/tmp/testb001.log", "0002:" + String.valueOf(test1));
        boolean test2 = checkIfDirectoryFileExists("/disk/test234");
        Test.write("/tmp/testb001.log", "0003:" + String.valueOf(test2));
        boolean test3 = deleteDirectory("/disk/test234");
        Test.write("/tmp/testb001.log", "0004:" + String.valueOf(test3));
        boolean test4 = checkIfDirectoryFileExists("/disk/test234");
        Test.write("/tmp/testb001.log", "0005:" + String.valueOf(test4));
        //Temporary... End...
        /*
        Test.write("/tmp/testb001.log", "001");
        writeFileExample();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ex) {
        }
        Test.write("/tmp/testb001.log", "002");
        for (Map.Entry<String, FileType> entry : listDirectoriesFilesByPath("/").entrySet()) {
            Test.write("/tmp/testb001.log", "003_1:" + entry.getKey() + "|" + entry.getValue());
        }
        for (Map.Entry<String, FileType> entry : listDirectoriesFilesByPath("/disk/").entrySet()) {
            Test.write("/tmp/testb001.log", "003_2:" + entry.getKey() + "|" + entry.getValue());
        }
        Test.write("/tmp/testb001.log", "004");
        readFileExample();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ex) {
        }
        Test.write("/tmp/testb001.log", "005");
        for (Map.Entry<String, FileType> entry : listDirectoriesFilesByPath("/").entrySet()) {
            Test.write("/tmp/testb001.log", "006_1:" + entry.getKey() + "|" + entry.getValue());
        }
        for (Map.Entry<String, FileType> entry : listDirectoriesFilesByPath("/disk/").entrySet()) {
            Test.write("/tmp/testb001.log", "006_2:" + entry.getKey() + "|" + entry.getValue());
        }
        Test.write("/tmp/testb001.log", "007");
        */
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

    private boolean isDirectory(String dirPath)
    {
        return checkIfDirectoryExists(dirPath);
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

    private String getParentDirectory(String path)
    {
        String result = "/";
        if (path != null) {
            String tempPath = path;
            if (path.endsWith("/")) {
                tempPath = path.substring(0, path.length() - 2);
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
        _log.error("TSTG:" + result);
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
        _log.error("TSTG0:" + result);
        return result;
    }

    private boolean checkIfDirectoryFileExists(String dirPath)
    {
        boolean result = false;
        String searchedItem = getItem(dirPath);
        String tmpDirPath = addPrefixSlashToPath(dirPath);
        Map<String, FileType> listing = listDirectoriesFilesByPath(getParentDirectory(tmpDirPath));
        for (Map.Entry<String, FileType> entry : listing.entrySet()) {
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
        Map<String, FileType> listing = listDirectoriesFilesByPath(tmpPath);
        for (Map.Entry<String, FileType> entry : listing.entrySet()) {
            if (entry.getValue() == DIR) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    private List<String> listFilesByPath(String path)
    {
        List<String> result = new ArrayList<>();
        String tmpPath = addPrefixSlashToPath(path);
        Map<String, FileType> listing = listDirectoriesFilesByPath(tmpPath);
        for (Map.Entry<String, FileType> entry : listing.entrySet()) {
            if (entry.getValue() == REGULAR) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    private Map<String, FileType> listDirectoriesFilesByPath(String path)
    {
        String tmpPath = addPrefixSlashToPath(path);
        FsPath fsPath = new FsPath(tmpPath);
        Map<String, FileType> result = new HashMap<>();
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
        Map<String, FileType> listing = listDirectoriesFilesByPath(tmpPath);
        for (Map.Entry<String, FileType> entry : listing.entrySet()) {
            if (entry.getValue() == REGULAR) {
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
        if (path != null) {
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
        if (path != null) {
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
        if (path != null) {
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

    private static class ListPrinter implements DirectoryListPrinter
    {
        private final Map<String, FileType> list;

        private ListPrinter(Map<String, FileType> list)
        {
            this.list = list;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.of(TYPE, SIZE);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
                throws InterruptedException
        {
            FileAttributes attr = entry.getFileAttributes();
            list.put(entry.getName(), attr.getFileType());
            Test.write("/tmp/listing.log", "Out:" + entry.getName() + "|" + String.valueOf(attr.getFileType()) + "|" + String.valueOf(attr.getSize())); //temporary
        }
    }

    //Minimum to write a file
    public void writeFileExample()
    {
        try {
            //The order of all commands is very important!
            String data = "Hello!";
            String filePath = "/disk/test2.txt";
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
            _log.error("CDMIContainerDaoImpl received data: " + data);
        } catch (CacheException | InterruptedException | UnknownHostException ex) {
            _log.error("File could not become read, exception is: " + ex.getMessage());
        }
    }

}