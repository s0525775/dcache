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

package org.dcache.cdmi.resource;

import java.net.URI;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;
import org.dcache.cdmi.model.DcacheContainer;
import org.dcache.cdmi.model.DcacheDataObject;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.dao.ContainerDao;
import org.snia.cdmiserver.dao.DataObjectDao;
import org.snia.cdmiserver.exception.ForbiddenException;
import org.snia.cdmiserver.model.Container;
import org.snia.cdmiserver.model.DataObject;
import org.snia.cdmiserver.util.MediaTypes;

/**
 * <p>
 * Access to objects by object Id.
 * </p>
 */
// @Path("/cdmi_objectid/{path}")
@Path("/cdmi_objectid/{objectId:.+}")
// How will URL get here ? TBD
public class DcacheObjectIdResource
{
    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcacheObjectIdResource.class);

    //
    // Properties and Dependency Injection Methods
    //
    private ContainerDao containerDao;
    private DataObjectDao dataObjectDao;

    /**
    * <p>
    * Injected {@link DCacheContainerDao} instance.
    * </p>
    * @param containerDao
    */
    public void setContainerDao(ContainerDao containerDao)
    {
        this.containerDao = containerDao;
    }

    /**
    * <p>
    * Injected {@link DCacheDataObjectDao} instance.
    * </p>
    * @param dataObjectDao
    */
    public void setDataObjectDao(DataObjectDao dataObjectDao)
    {
        this.dataObjectDao = dataObjectDao;
    }

    /**
    * <p>
    * [9.4] Read a Container Object (CDMI Content Type)
    * [8.4] Read a Data Object (CDMI Content Type)
    * </p>
    *
    * @param objectId
    * @param uriInfo
    * @param headers
    * @return
    */

    @GET
    @Consumes(MediaTypes.OBJECT)
    public Response getContainerOrDataObjectByID(
            @PathParam("objectId") String objectId,
            @Context UriInfo uriInfo,
            @Context HttpHeaders headers)
    {
        System.out.println("In DcacheObjectIdResource.getContainerOrObjectByID, path=" + objectId);
        _log.trace("In DcacheObjectIdResource.getContainerOrObjectByID, path={}", objectId);

        //print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
          _log.trace("Hdr: {} - {}", hdr, headers.getRequestHeader(hdr));
        }

        String path = "/cdmi_objectid/" + objectId;
        if (headers.getRequestHeader(HttpHeaders.CONTENT_TYPE).isEmpty()) {
          return getDataObjectOrContainerByID(path,uriInfo,headers);
        }

        String theObjectId = "";
        String restPath = "";
        int slashIndex = objectId.indexOf("/");
        if (slashIndex > 0) {
            theObjectId = objectId.substring(0, slashIndex);
            restPath = objectId.substring(slashIndex + 1);
        } else {
            theObjectId = objectId;
        }
        System.out.println("001: " + objectId);
        System.out.println("002: " + restPath);
        if (!restPath.isEmpty()) {
            // Check for container vs object
            if (containerDao.isContainer(path)) {
                // if container build container browser page
                try {
                    Container container = (DcacheContainer) containerDao.findByPath(path);
                    if (container == null) {
                        ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity("Not Found").build();
                    } else {
                        String respStr = container.toJson(false);
                        ResponseBuilder builder = Response.ok();
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity(respStr).build();
                    }
                } catch (ForbiddenException ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Container Read Error: " + ex.toString()).build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Container Read Error: " + ex.toString()).build();
                }
            } else {
                // if object, send out the object in it's native form
                try {
                    DataObject dObj = (DcacheDataObject) dataObjectDao.findByPath(path);
                    if (dObj == null) {
                        ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity("Not Found").build();
                    } else {
                        // make http response
                        // build a JSON representation
                        //String respStr = dObj.getValue();//Remark: Switch to dObj.toJson() if Metadata shall be showed instead
                        String respStr = dObj.toJson();
                        _log.trace("MimeType={}", dObj.getMimetype());
                        ResponseBuilder builder = Response.ok(new URI(path));
                        builder.type(dObj.getMimetype());
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity(respStr).build();
                    } // if/else
                } catch (ForbiddenException ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Object Fetch Error: " + ex.toString()).build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Object Fetch Error: " + ex.toString()).build();
                }
            }
        } else {
            // Check for container vs object
            if (containerDao.isContainer(path)) {
                // if container build container browser page
                try {
                    Container container = (DcacheContainer) containerDao.findByObjectId(theObjectId);
                    if (container == null) {
                        ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity("Not Found").build();
                    } else {
                        String respStr = container.toJson(false);
                        ResponseBuilder builder = Response.ok();
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity(respStr).build();
                    }
                } catch (ForbiddenException ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Container Read Error: " + ex.toString()).build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Container Read Error: " + ex.toString()).build();
                }
            } else {
                // if object, send out the object in it's native form
                try {
                    DataObject dObj = (DcacheDataObject) dataObjectDao.findByObjectId(theObjectId);
                    if (dObj == null) {
                        ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity("Not Found").build();
                    } else {
                        // make http response
                        // build a JSON representation
                        //String respStr = dObj.getValue();//Remark: Switch to dObj.toJson() if Metadata shall be showed instead
                        String respStr = dObj.toJson();
                        _log.trace("MimeType={}", dObj.getMimetype());
                        ResponseBuilder builder = Response.ok(new URI(path));
                        builder.type(dObj.getMimetype());
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity(respStr).build();
                    } // if/else
                } catch (ForbiddenException ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Object Fetch Error: " + ex.toString()).build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Object Fetch Error: " + ex.toString()).build();
                }
            }
        }
    }

    /**
    * <p>
    * [8.5] Read a Data Object (Non-CDMI Content Type)
    * [9.5] Read a Container Object (Non-CDMI Content Type)
    * </p>
    *
    * <p>
    * IMPLEMENTATION NOTE - Consult <code>uriInfo.getQueryParameters()</code> to identify
    * restrictions on the returned information.
    * </p>
    *
    * <p>
    * IMPLEMENTATION NOTE - If the path points at a container,
    * the response content type must be"text/json".
    * </p>
    *
    * @param objectId
     * @param uriInfo
    * @param headers
    * @return
    */
    @GET
    public Response getDataObjectOrContainerByID(
            @PathParam("objectId") String objectId,
            @Context UriInfo uriInfo,
            @Context HttpHeaders headers)
    {

        System.out.println("In DcacheObjectIdResource.getDataObjectOrContainerByID, path=" + objectId);
        _log.trace("In DcacheObjectIdResource.getDataObjectOrContainerByID, path={}", objectId);
        String query = uriInfo.getRequestUri().getQuery();

        // print queryparams for debug - TODO! See http://cdmi.sniacloud.com/cdmi_spec/9-container_objects/9-container_objects.htm (chapter 9.4.1)
        if (query != null) {
            if (!query.isEmpty()) {
                String queries[] = query.split(";");
                for (String item : queries) {
                    if (item != null) {
                        if (item.isEmpty()) {
                            String values[] = item.split(":");
                            if (values.length > 1) {
                                _log.trace("Query: {} - {}", values[0], values[1]);
                            } else if (values.length > 0) {
                                _log.trace("Query: {}", values[0]);
                            }
                        }
                    }
                }
            }
        }

        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("Hdr: {} - {}", hdr, headers.getRequestHeader(hdr));
        }

        String path = "";
        String theObjectId = "";
        String restPath = "";
        if (!objectId.startsWith("/cdmi_objectid/")) {
            path = "/cdmi_objectid/" + objectId;
        } else {
            path = objectId;
        }
        String testPath = objectId;
        if (objectId.startsWith("/cdmi_objectid/") || objectId.startsWith("cdmi_objectid/")) {
            if (objectId.startsWith("/cdmi_objectid/")) {
                testPath = objectId.replace("/cdmi_objectid/", "");
            } else {
                testPath = objectId.replace("cdmi_objectid/", "");
            }
        }
        int slashIndex = testPath.indexOf("/");
        if (slashIndex > 0) {
            theObjectId = testPath.substring(0, slashIndex);
            restPath = testPath.substring(slashIndex + 1);
        } else {
            theObjectId = testPath;
        }
        if (!restPath.isEmpty()) {
            // Check for container vs object
            if (containerDao.isContainer(path)) {
                // if container build container browser page
                try {
                    Container container = (DcacheContainer) containerDao.findByPath(path);
                    if (container == null) {
                        ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity("Not Found").build();
                    } else {
                        String respStr = container.toJson(false);
                        ResponseBuilder builder = Response.ok();
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity(respStr).build();
                    }
                } catch (ForbiddenException ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Container Read Error: " + ex.toString()).build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Container Read Error: " + ex.toString()).build();
                }
            } else {
                // if object, send out the object in it's native form
                try {
                    DataObject dObj = (DcacheDataObject) dataObjectDao.findByPath(path);
                    if (dObj == null) {
                        ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity("Not Found").build();
                    } else {
                        // make http response
                        // build a JSON representation
                        //String respStr = dObj.getValue();//Remark: Switch to dObj.toJson() if Metadata shall be showed instead
                        String respStr = dObj.toJson();
                        _log.trace("MimeType={}", dObj.getMimetype());
                        ResponseBuilder builder = Response.ok(new URI(path));
                        builder.type(dObj.getMimetype());
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity(respStr).build();
                    } // if/else
                } catch (ForbiddenException ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Object Fetch Error: " + ex.toString()).build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Object Fetch Error: " + ex.toString()).build();
                }
            }
        } else {
            // Check for container vs object
            if (containerDao.isContainer(path)) {
                // if container build container browser page
                try {
                    Container container = (DcacheContainer) containerDao.findByObjectId(theObjectId);
                    if (container == null) {
                        ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity("Not Found").build();
                    } else {
                        String respStr = container.toJson(false);
                        ResponseBuilder builder = Response.ok();
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity(respStr).build();
                    }
                } catch (ForbiddenException ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Container Read Error: " + ex.toString()).build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Container Read Error: " + ex.toString()).build();
                }
            } else {
                // if object, send out the object in it's native form
                try {
                    DataObject dObj = (DcacheDataObject) dataObjectDao.findByObjectId(theObjectId);
                    if (dObj == null) {
                        ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity("Not Found").build();
                    } else {
                        // make http response
                        // build a JSON representation
                        //String respStr = dObj.getValue();//Remark: Switch to dObj.toJson() if Metadata shall be showed instead
                        String respStr = dObj.toJson();
                        _log.trace("MimeType={}", dObj.getMimetype());
                        ResponseBuilder builder = Response.ok(new URI(path));
                        builder.type(dObj.getMimetype());
                        builder.header("X-CDMI-Specification-Version", "1.0.2");
                        return builder.entity(respStr).build();
                    } // if/else
                } catch (ForbiddenException ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Object Fetch Error: " + ex.toString()).build();
                } catch (Exception ex) {
                    ex.printStackTrace();
                    _log.trace(ex.toString());
                    ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity("Object Fetch Error: " + ex.toString()).build();
                }
            }
        }
    }

    /**
    * <p>
    * [8.2] Create a Data Object (CDMI Content Type)
    * [8.6] Update Data Object (CDMI Content Type)
    * </p>
    *
    * @param headers
    * @param objectId
    * @param bytes
    * @return
    */
    @PUT
    @Consumes(MediaTypes.DATA_OBJECT)
    @Produces(MediaTypes.DATA_OBJECT)
    public Response putDataObjectByID(
            @Context HttpHeaders headers,
            @PathParam("objectId") String objectId,
            byte[] bytes)
    {

        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("Hdr: {} - {}", hdr, headers.getRequestHeader(hdr));
        }
        String inBuffer = new String(bytes);
        System.out.println("Object Id = " + objectId + "\n" + inBuffer);
        String objectPath = "/cdmi_objectid/" + objectId;
        System.out.println("In DcacheObjectIdResource.putDataObjectByID, path=" + objectPath);
        _log.trace("In DcacheObjectIdResource.putDataObjectByID, path={}", objectPath);

        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("{} - {}", hdr, headers.getRequestHeader(hdr));
        }

        try {
            DataObject dObj = (DcacheDataObject) dataObjectDao.findByPath(objectPath);
            if (dObj == null) {
                dObj = new DcacheDataObject();

                dObj.setObjectType("application/cdmi-object");
                // parse json
                dObj.fromJson(bytes, false);
                if (dObj.getValue() == null) {
                    dObj.setValue("== N/A ==");
                }
                if (dObj.getMove() == null) {
                    dObj = (DcacheDataObject) dataObjectDao.createByPath(objectPath, dObj);
                    // return representation
                    String respStr = dObj.toJson();
                    // make http response
                    // build a JSON representation
                    System.out.println("Created");
                    ResponseBuilder builder = Response.created(new URI(objectPath));
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity(respStr).build();
                } else {
                    dObj = (DcacheDataObject) dataObjectDao.createByPath(objectPath, dObj);
                    // return representation
                    String respStr = dObj.toJson();
                    // make http response
                    // build a JSON representation
                    System.out.println("Ok");
                    ResponseBuilder builder = Response.ok(new URI(objectPath));
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity(respStr).build();
                }
            } else {
                dObj = new DcacheDataObject();

                dObj.setObjectType("application/cdmi-object");
                // parse json
                dObj.fromJson(bytes, false);
                if (dObj.getValue() == null) {
                    dObj.setValue("== N/A ==");
                }
                dObj = (DcacheDataObject) dataObjectDao.createByPath(objectPath, dObj);
                // return representation
                String respStr = dObj.toJson();
                // make http response
                // build a JSON representation
                System.out.println("Ok");
                ResponseBuilder builder = Response.ok(new URI(objectPath));
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                return builder.entity(respStr).build();
            }
        } catch (ForbiddenException ex) {
            ex.printStackTrace();
            _log.trace(ex.toString());
                ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                return builder.entity("Object Creation Error: " + ex.toString()).build();
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.trace(ex.toString());
                ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                return builder.entity("Object Creation Error: " + ex.toString()).build();
        }
    }

    /**
    * <p>
    * [9.2] Create a Container (CDMI Content Type) and
    * [9.6] Update a Container (CDMI Content Type)
    * </p>
    *
    * @param headers
    * @param objectId
    * @param bytes
    * @return
    */
    @PUT
    @Consumes(MediaTypes.CONTAINER)
    @Produces(MediaTypes.CONTAINER)
    public Response putContainerByID(
            @Context HttpHeaders headers,
            @PathParam("objectId") String objectId,
            byte[] bytes)
    {
        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("Hdr: {} - {}", hdr, headers.getRequestHeader(hdr));
        }
        String inBuffer = new String(bytes);
        System.out.println("Object Id = " + objectId + "\n" + inBuffer);
        String path = "/cdmi_objectid/" + objectId;
        System.out.println("In DcacheObjectIDResource.putContainerByID, path=" + path);
        _log.trace("In DcacheObjectIDResource.putContainerByID, path={}", path);

        Container containerRequest = new DcacheContainer();

        try {
            containerRequest.fromJson(bytes, false);
            Container container = (DcacheContainer) containerDao.createByPath(path, containerRequest);
            // return representation
            String respStr = container.toJson(false);
            // make http response
            // build a JSON representation
            System.out.println("Ok");
            ResponseBuilder builder = Response.ok(new URI(path));
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity(respStr).build();
        } catch (ForbiddenException ex) {
            ex.printStackTrace();
            _log.trace(ex.toString());
            ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Container Creation Error: " + ex.toString()).build();
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.trace(ex.toString());
            ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Container Creation Error: " + ex.toString()).build();
        }
    }

    @PUT
    // @Consumes("application/json")
    @Consumes(MediaTypes.CONTAINER)
    @Produces(MediaTypes.CONTAINER)
    public Response updateContainerByID(
            @Context HttpHeaders headers,
            @PathParam("objectId") String objectId,
            byte[] bytes) {
        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("Hdr: {} - {}", hdr, headers.getRequestHeader(hdr));
        }

        String inBuffer = new String(bytes);
        System.out.println("Object Id = " + objectId + "\n" + inBuffer);
        String objectPath = "/cdmi_objectid/" + objectId;
        System.out.println("In DcacheObjectIdResource.updateContainerByID, path=" + objectPath);
        _log.trace("In DcacheObjectIdResource.updateContainerByID, path={}", objectPath);

        Container containerRequest = new DcacheContainer();

        try {
            containerRequest.fromJson(bytes, false);
            Container container = (DcacheContainer) containerDao.createByPath(objectPath,
                    containerRequest);
            if (container == null) {
                ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                return builder.entity("Container Read Error").build();
            } else {
                // make http response
                // build a JSON representation
                String respStr = container.toJson(false);
                ResponseBuilder builder = Response.created(new URI(objectPath));
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                return builder.entity(respStr).build();
            } // if/else
        } catch (ForbiddenException ex) {
            ex.printStackTrace();
            _log.trace(ex.toString());
            ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Container Creation Error: " + ex.toString()).build();
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.trace(ex.toString());
            ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Container Creation Error: " + ex.toString()).build();
        }
    }

    @POST
    // @Consumes("application/json")
    @Consumes(MediaTypes.DATA_OBJECT)
    @Produces(MediaTypes.DATA_OBJECT)
    public Response createDataObjectByID(
            @Context HttpHeaders headers,
            @PathParam("objectId") String objectId,
            byte[] bytes) {
        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("Hdr: {} - {}", hdr, headers.getRequestHeader(hdr));
        }
        String inBuffer = new String(bytes);
        System.out.println("Object Id = " + objectId + "\n" + inBuffer);
        DcachePathResource pathResource = new DcachePathResource();
        String objectPath = "/cdmi_objectid/" + objectId;
        System.out.println("In DcacheObjectIdResource.createDataObjectByID, path=" + objectPath);
        _log.trace("In DcacheObjectIdResource.createDataObjectByID, path={}", objectPath);
        Response resp = pathResource.postDataObject(objectPath,bytes);
        return resp;
    }

    @DELETE
    // @Consumes("application/json")
    public Response deleteDataObjectOrContainerByID(
            @Context HttpHeaders headers,
            @PathParam("objectId") String objectId,
            byte[] bytes) {
        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("Hdr: {} - {}", hdr, headers.getRequestHeader(hdr));
        }
        String inBuffer = new String(bytes);
        System.out.println("Object Id = " + objectId + "\n" + inBuffer);
        String objectPath = "/cdmi_objectid/" + objectId;
        System.out.println("In DcacheObjectIdResource.deleteDataObjectByID, path=" + objectPath);
        _log.trace("In DcacheObjectIdResource.deleteDataObjectByID, path={}", objectPath);
        try {
            containerDao.deleteByPath(objectPath);
            ResponseBuilder builder = Response.status(Response.Status.NO_CONTENT);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("No Content").build();
        } catch (ForbiddenException ex) {
            ex.printStackTrace();
            _log.trace(ex.toString());
            ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Object Delete Error: " + ex.toString()).build();
        } catch (Exception ex) {
            ex.printStackTrace();
            _log.trace(ex.toString());
            ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Object Delete Error: " + ex.toString()).build();
        }
    }
}
