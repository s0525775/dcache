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
package org.dcache.cdmi.resource;

import com.google.common.collect.ImmutableList;
import java.net.URI;
import java.net.URISyntaxException;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.PUT;
import javax.ws.rs.POST;
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
import org.snia.cdmiserver.util.ObjectID;


/**
* <p>
* Access to objects by path.
* </p>
*/
public class DcachePathResource
{

    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcachePathResource.class);

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

    //
    // Resource Methods
    //
    /**
    * <p>
    * [8.8] Delete a Data Object and
    * [9.7] Delete a Container Object
    * </p>
    *
    * @param path
    * Path to the existing object
    * @return
    */
    @DELETE
    @Path("/{path:.+}")
    public Response deleteDataObjectOrContainer(@PathParam("path") String path)
    {

        try {
            containerDao.deleteByPath(path);
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

    /**
    * <p>
    * Trap to catch attempts to delete the root container
    * </p>
    *
    * @param path
    * Path to the existing data object
    * @return
    */
    @DELETE
    @Path("/")
    public Response deleteRootContainer(@PathParam("path") String path)
    {
        ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
        builder.header("X-CDMI-Specification-Version", "1.0.2");
        return builder.entity("Can not delete root container").build();
    }

    /**
    * <p>
    * [9.4] Read a Container Object (CDMI Content Type)
    * [8.4] Read a Data Object (CDMI Content Type)
    * </p>
    *
    * @param path
    * Path to the existing non-root container
     * @param uriInfo
    * @param headers
    * @return
    */

    @GET
    @Path("/{path:.+}")
    @Consumes(MediaTypes.OBJECT)
    public Response getContainerOrDataObject(
            @PathParam("path") String path,
            @Context UriInfo uriInfo,
            @Context HttpHeaders headers)
    {
        _log.trace("In DcachePathResource.getContainerOrObject, path={}", path);

        //print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
          _log.trace("Hdr: {} - {}", hdr, headers.getRequestHeader(hdr));
        }

        if (headers.getRequestHeader(HttpHeaders.CONTENT_TYPE).isEmpty()) {
          return getDataObjectOrContainer(path, uriInfo, headers);
        }

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
              ResponseBuilder builder = Response.ok(new URI(path));
              builder.header("X-CDMI-Specification-Version", "1.0.2");
              return builder.entity(respStr).build();
            }
         } catch (ForbiddenException ex) {
            _log.trace(ex.toString());
            ResponseBuilder builder = Response.status(Response.Status.FORBIDDEN);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Container Read Error: " + ex.toString()).build();
          } catch (URISyntaxException ex) {
            _log.trace(ex.toString());
            ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Container Read Error: " + ex.toString()).build();
          }
        }
        try {
          DataObject dObj = (DcacheDataObject) dataObjectDao.findByPath(path);
          if (dObj == null) {
            ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Not Found").build();
          } else {
            // make http response
            // build a JSON representation
            String respStr = dObj.toJson();
            ResponseBuilder builder = Response.ok(new URI(path));
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

    /**
    * <p>
    * [9.4] Read a Container Object (CDMI Content Type).
    * Catches request routing for root container in spite of CXF bug.
    * </p>
    *
    * @param path
    * Path to the root container
     * @param uriInfo
    * @param headers
    * @return
    */
    @GET
    @Path("/")
    @Consumes(MediaTypes.CONTAINER)
    public Response getRootContainer(
            @PathParam("path") String path,
            @Context UriInfo uriInfo,
            @Context HttpHeaders headers)
    {

        _log.trace("In DcachePathResource.getRootContainer");
        return getContainerOrDataObject(path, uriInfo, headers);

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
    * @param path
    * Path to the existing data object or container
     * @param uriInfo
    * @param headers
    * @return
    */
    @GET
    @Path("/{path:.+}")
    public Response getDataObjectOrContainer(
            @PathParam("path") String path,
            @Context UriInfo uriInfo,
            @Context HttpHeaders headers)
    {

        System.out.println("In DcachePathResource.getDataObjectOrContainer, path=" + path);
        _log.trace("In DcachePathResource.getDataObjectOrContainer, path={}", path);
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
    }

    /**
    * <p>
    * [9.2] Create a Container (CDMI Content Type) and
    * [9.6] Update a Container (CDMI Content Type)
    * </p>
    *
    * @param path
    * Path to the new or existing container
    * @param noClobber
    * Value of the no-clobber header (or "false" if not present)
    * @param mustExist
    * Value of the must-exist header (or "false" if not present)
    * @param bytes
    * @return
    */
    @PUT
    @Path("/{path:.+}/")
    @Consumes(MediaTypes.CONTAINER)
    @Produces(MediaTypes.CONTAINER)
    public Response putContainer(
            @PathParam("path") String path,
            @HeaderParam("X-CDMI-NoClobber") @DefaultValue("false") String noClobber,
            @HeaderParam("X-CDMI-MustExist") @DefaultValue("false") String mustExist,
            byte[] bytes)
    {
        _log.trace("In DcachePathResource.putContainer, path={}", path);

        String inBuffer = new String(bytes);
        _log.trace("Request={}", inBuffer);

        Container containerRequest = new DcacheContainer();

        try {
            containerRequest.fromJson(bytes, false);
            Container container = (DcacheContainer) containerDao.createByPath(path,
                    containerRequest);
            if (container == null) {
                ResponseBuilder builder = Response.status(Response.Status.BAD_REQUEST);
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                return builder.entity("Container Read Error").build();
            } else {
                // make http response
                // build a JSON representation
                String respStr = container.toJson(false);
                ResponseBuilder builder = Response.created(new URI(path));
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

    /**
    * <p>
    * [8.2] Create a Data Object (CDMI Content Type)
    * [8.6] Update Data Object (CDMI Content Type)
    * </p>
    *
    * @param headers
    * @param path
    * Path to the parent container for the new data object
    * @param bytes
    * @return
    */
    @PUT
    @Path("/{path:.+}")
    @Consumes(MediaTypes.DATA_OBJECT)
    @Produces(MediaTypes.DATA_OBJECT)
    public Response putDataObject(
            @Context HttpHeaders headers,
            @PathParam("path") String path,
            byte[] bytes)
    {

        _log.trace("putDataObject():");
        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("{} - {}", hdr, headers.getRequestHeader(hdr));
        }
        String inBuffer = new String(bytes);
        _log.trace("Path={}\n{}", path, inBuffer);

        try {
            DataObject dObj = (DcacheDataObject) dataObjectDao.findByPath(path);
            if (dObj == null) {
                dObj = new DcacheDataObject();

                dObj.setObjectType("application/cdmi-object");
                // parse json
                dObj.fromJson(bytes, false);
                if (dObj.getValue() == null) {
                    dObj.setValue("== N/A ==");
                }
                if (dObj.getMove() == null) {
                    dObj = (DcacheDataObject) dataObjectDao.createByPath(path, dObj);
                    // return representation
                    String respStr = dObj.toJson();
                    // make http response
                    // build a JSON representation
                    System.out.println("Created");
                    ResponseBuilder builder = Response.created(new URI(path));
                    builder.header("X-CDMI-Specification-Version", "1.0.2");
                    return builder.entity(respStr).build();
                } else {
                    dObj = (DcacheDataObject) dataObjectDao.createByPath(path, dObj);
                    // return representation
                    String respStr = dObj.toJson();
                    // make http response
                    // build a JSON representation
                    System.out.println("Ok");
                    ResponseBuilder builder = Response.ok(new URI(path));
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
                dObj = (DcacheDataObject) dataObjectDao.createByPath(path, dObj);
                // return representation
                String respStr = dObj.toJson();
                // make http response
                // build a JSON representation
                System.out.println("Ok");
                ResponseBuilder builder = Response.ok(new URI(path));
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
    * [8.3] Create a new Data Object (Non-CDMI Content Type) and
    * [8.7] Update a Data Object (Non-CDMI Content Type)
    * </p>
    *
    * @param path
    * Path to the new or existing data object
    * @param contentType
    * @param bytes
    * @return
    */
    @PUT
    @Path("/{path:.+}")
    public Response putDataObject(
            @PathParam("path") String path,
            @HeaderParam("Content-Type") String contentType,
            byte[] bytes)
    {

        throw new UnsupportedOperationException(
                "DcachePathResource.putDataObject(Non-CDMI Content Type");
    }

    /**
    * <p>
    * [9.10] Create a New Data Object (NON-CDMI Content Type)
    * </p>
    *
    * @param
    * path Path to the new or existing data object
    * @param bytes
    * @return
    */
    @Path("/{path:.+}")
    @POST
    public Response postDataObject(
            @PathParam("path") String path,
            byte[] bytes)
    {

        String inBuffer = new String(bytes);
        _log.trace("Path={}\n{}", path, inBuffer);

        boolean containerRequest = false;
        if (containerDao.isContainer(path)) {
            containerRequest = true;
        }

        try {
            String objectId = ObjectID.getObjectID(11);
            String objectPath = path + "/" + objectId;

            DataObject dObj = new DcacheDataObject();
            dObj.setObjectID(objectId);
            dObj.setObjectType(objectPath);
            dObj.setValue(inBuffer);

            _log.trace("objectId={}, objectPath={}", objectId, objectPath);

            dObj = (DcacheDataObject) dataObjectDao.createByPath(objectPath, dObj);

            if (containerRequest) {
                ResponseBuilder builder = Response.ok(new URI(path));
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                builder.header("Location", dObj.getObjectType());
                return builder.build();
            }
            return Response.ok().build();
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
    * [8.2] Create a Data Object (CDMI Content Type)
    * [8.6] Update Data Object (CDMI Content Type)
    * </p>
    *
    * @param headers
    * @param path
    * Path to the parent container for the new data object
    * @param bytes
    * @return
    */
    @POST
    @Path("/{path:.+}")
    @Consumes(MediaTypes.DATA_OBJECT)
    @Produces(MediaTypes.DATA_OBJECT)
    public Response postDataObject(
            @Context HttpHeaders headers,
            @PathParam("path") String path,
            byte[] bytes)
    {

        _log.trace("postDataObject():");
        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            _log.trace("{} - {}", hdr, headers.getRequestHeader(hdr));
        }
        String inBuffer = new String(bytes);
        _log.trace("Path={}\n{}", path, inBuffer);

        boolean containerRequest = false;
        if (containerDao.isContainer(path)) {
            containerRequest = true;
        }

        try {
            String objectId = ObjectID.getObjectID(11);
            String objectPath = path + "/" + objectId;

            DataObject dObj = new DcacheDataObject();
            dObj.setObjectID(objectId);
            dObj.setObjectType(objectPath);
            dObj.setValue(inBuffer);

            _log.trace("objectId={}, objectPath={}", objectId, objectPath);

            dObj = (DcacheDataObject) dataObjectDao.createByPath(objectPath, dObj);

            if (containerRequest) {
                ResponseBuilder builder = Response.ok(new URI(path));
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                builder.header("Location", dObj.getObjectType());
                return builder.build();
            }
            return Response.ok().build();
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
    * [9.3] Create a Container (Non-CDMI Content Type)
    * </p>
    *
    * <p>
    * FIXME - I do not see how to disambiguate this kind of call from
    * creating a data object with a non-CDMI Content Type)
    */

}