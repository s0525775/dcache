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
import org.dcache.cdmi.mover.CDMIDataTransfer;
import org.dcache.cdmi.temp.Test;

import org.snia.cdmiserver.dao.ContainerDao;
import org.snia.cdmiserver.dao.DataObjectDao;
import org.snia.cdmiserver.model.Container;
import org.snia.cdmiserver.model.DataObject;
import org.snia.cdmiserver.util.MediaTypes;
import org.snia.cdmiserver.util.ObjectID;


/**
 * <p>
 * Access to objects by path.
 * </p>
 */
public class PathResource {

    //
    // Properties and Dependency Injection Methods
    //
    private ContainerDao containerDao;

    /**
     * <p>
     * Injected {@link ContainerDao} instance.
     * </p>
     */
    public void setContainerDao(
            ContainerDao containerDao) {
        this.containerDao = containerDao;
    }

    private DataObjectDao dataObjectDao;

    /**
     * <p>
     * Injected {@link DataObjectDao} instance.
     * </p>
     */
    public void setDataObjectDao(
            DataObjectDao dataObjectDao) {
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
     *            Path to the existing object
     */
    @DELETE
    @Path("/{path:.+}")
    public Response deleteDataObjectOrContainer(
            @PathParam("path") String path) {

        try {
            containerDao.deleteByPath(path);
            return Response.ok().header(
                    "X-CDMI-Specification-Version", "1.0.2").build();
        } catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).tag(
                    "Object Delete Error : " + ex.toString()).build();
        }
    }

    /**
     * <p>
     * Trap to catch attempts to delete the root container
     * </p>
     *
     * @param path
     *            Path to the existing data object
     */
    @DELETE
    @Path("/")
    public Response deleteRootContainer(@PathParam("path") String path) {
        return Response.status(Response.Status.BAD_REQUEST).tag(
                "Can not delete root container").build();
    }

    /**
     * <p>
     * [9.4] Read a Container Object (CDMI Content Type)
     * [8.4] Read a Data Object (CDMI Content Type)
     * </p>
     *
     * @param path
     *            Path to the existing non-root container
     */

    @GET
    @Path("/{path:.+}")
    @Consumes(MediaTypes.OBJECT)
    public Response getContainerOrDataObject(
            @PathParam("path") String path,
            @Context HttpHeaders headers) {

        System.out.println("In PathResource.getContainerOrObject, path=" +
                path);

        //print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
          System.out.println("Hdr: "+ hdr + " - " +
                  headers.getRequestHeader(hdr));
        }

        Test.write("/tmp/testd002.log", "Test001");
        if (headers.getRequestHeader(HttpHeaders.CONTENT_TYPE).isEmpty()) {
          Test.write("/tmp/testd002.log", "Test002");
          return getDataObjectOrContainer(path,headers);
        }

        Test.write("/tmp/testd002.log", "Test003");
        // Check for container vs object
        if (containerDao.isContainer(path)) {
          Test.write("/tmp/testd002.log", "Test004");
          // if container build container browser page
          try {
            Test.write("/tmp/testd002.log", "Test005");
            Container container = containerDao.findByPath(path);
            Test.write("/tmp/testd002.log", "Test006");
            if (container == null) {
              Test.write("/tmp/testd002.log", "Test007");
              return Response.status(Response.Status.NOT_FOUND).build();
            } else {
              Test.write("/tmp/testd002.log", "Test008");
              String respStr = container.toJson(false);
              Test.write("/tmp/testd002.log", "Test009");
              return Response.ok(respStr).header(
                      "X-CDMI-Specification-Version", "1.0.2").build();
            }
          } catch (Exception ex) {
            Test.write("/tmp/testd002.log", "Test010");
            System.out.println(ex);
            ex.printStackTrace();
            return Response.status(Response.Status.NOT_FOUND).tag(
                    "Container Read Error : " + ex.toString()).build();
          }
        }
        Test.write("/tmp/testd002.log", "Test011");
        try {
          Test.write("/tmp/testd002.log", "Test012");
          DataObject dObj = dataObjectDao.findByPath(path);
          Test.write("/tmp/testd002.log", "Test013");
          if (dObj == null) {
            Test.write("/tmp/testd002.log", "Test014");
            return Response.status(Response.Status.NOT_FOUND).build();
          } else {
            Test.write("/tmp/testd002.log", "Test015");
            // make http response
            // build a JSON representation
            String respStr = dObj.toJson();
            Test.write("/tmp/testd002.log", "Test016");
            //ResponseBuilder builder = Response.status(Response.Status.CREATED)
            return Response.ok(respStr).header(
                    "X-CDMI-Specification-Version", "1.0.2").build();
          } // if/else
        } catch (Exception ex) {
          Test.write("/tmp/testd002.log", "Test017");
          System.out.println(ex);
          ex.printStackTrace();
          return Response.status(Response.Status.BAD_REQUEST).tag(
                  "Object Fetch Error : " + ex.toString()).build();
        }
    }

    /**
     * <p>
     * [9.4] Read a Container Object (CDMI Content Type).
     *       Catches request routing for root container in spite of CXF bug.
     * </p>
     *
     * @param path
     *            Path to the root container
     */
    @GET
    @Path("/")
    @Consumes(MediaTypes.CONTAINER)
    public Response getRootContainer(
            @PathParam("path") String path,
            @Context HttpHeaders headers) {

        //Temp Helper Part
        //if (path == null) path = "/";

        System.out.println("In PathResource.getRootContainer, path: " +
                path);
        return getContainerOrDataObject(path, headers);

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
     *            Path to the existing data object or container
     * @param range
     *            Range header value (if specified), else empty string
     */
    @GET
    @Path("/{path:.+}")
    public Response getDataObjectOrContainer(
            @PathParam("path") String path,
            @Context HttpHeaders headers) {

        System.out.println("In PathResource.getDataObjectOrContainer, path: " +
                path);

        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            System.out.println("Hdr: " + hdr + " - " +
                    headers.getRequestHeader(hdr));
        }

        Test.write("/tmp/testd002.log", "Test018");
        // Check for container vs object
        if (containerDao.isContainer(path)) {
           Test.write("/tmp/testd002.log", "Test019");
            // if container build container browser page
            try {
                Test.write("/tmp/testd002.log", "Test020");
                Container container = containerDao.findByPath(path);
                Test.write("/tmp/testd002.log", "Test021");
                if (container == null) {
                    Test.write("/tmp/testd002.log", "Test022");
                    return Response.status(Response.Status.NOT_FOUND).build();
                } else {
                    Test.write("/tmp/testd002.log", "Test023");
                    String respStr = container.toJson(false);
                    Test.write("/tmp/testd002.log", "Test024");
                    return Response.ok(respStr).header(
                            "X-CDMI-Specification-Version", "1.0.2").build();
                }
            } catch (Exception ex) {
                Test.write("/tmp/testd002.log", "Test025");
                System.out.println(ex);
                ex.printStackTrace();
                return Response.status(Response.Status.NOT_FOUND)
                        .tag("Container Read Error : " + ex.toString()).build();
            }
        } else {
            Test.write("/tmp/testd002.log", "Test026");
            // if object, send out the object in it's native form
            try {
                Test.write("/tmp/testd002.log", "Test027");
                DataObject dObj = dataObjectDao.findByPath(path);
                if (dObj == null) {
                    Test.write("/tmp/testd002.log", "Test028");
                    return Response.status(Response.Status.NOT_FOUND).build();
                } else {
                    Test.write("/tmp/testd002.log", "Test029");
                    // make http response
                    // build a JSON representation
                    String respStr = dObj.getValue();// dObj.toJson();
                    Test.write("/tmp/testd002.log", "Test030");
                    System.out.println("MimeType = " + dObj.getMimetype());
                    Test.write("/tmp/testd002.log", "Test031");
                    return Response.ok(respStr).type(dObj.getMimetype()).header(
                            "X-CDMI-Specification-Version", "1.0.2").build();
                } // if/else
            } catch (Exception ex) {
                Test.write("/tmp/testd002.log", "Test032");
                System.out.println(ex);
                ex.printStackTrace();
                return Response.status(Response.Status.BAD_REQUEST)
                        .tag("Object Fetch Error : " + ex.toString()).build();
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
     *            Path to the new or existing container
     * @param noClobber
     *            Value of the no-clobber header (or "false" if not present)
     * @param mustExist
     *            Value of the must-exist header (or "false" if not present)
     */
    @PUT
    @Path("/{path:.+}/")
    @Consumes(MediaTypes.CONTAINER)
    @Produces(MediaTypes.CONTAINER)
    public Response putContainer(
            @PathParam("path") String path,
            @HeaderParam("X-CDMI-NoClobber") @DefaultValue("false") String noClobber,
            @HeaderParam("X-CDMI-MustExist") @DefaultValue("false") String mustExist,
            byte[] bytes) {

        System.out.println("In PathResource.putContainer, path is: " + path);

        String inBuffer = new String(bytes);
        System.out.println("Request = " + inBuffer);

        Container containerRequest = new Container();

        try {
            containerRequest.fromJson(bytes, false);
            Container container = containerDao.createByPath(path,
                    containerRequest);
            if (container == null) {
                return Response.status(Response.Status.BAD_REQUEST).build();
            } else {
                // make http response
                // build a JSON representation
                String respStr = container.toJson(false);
                ResponseBuilder builder = Response.created(new URI(path));
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                //ResponseBuilder builder = Response.status(Response.Status.CREATED);
                return builder.entity(respStr).build();
                /* return Response.created(respStr).header(
                        "X-CDMI-Specification-Version", "1.0.2").build(); */
            } // if/else
        } catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST)
                    .tag("Object Creation Error : " + ex.toString()).build();
        }
    }

    /**
     * <p>
     * [8.2] Create a Data Object (CDMI Content Type)
     * [8.6] Update Data Object (CDMI Content Type)
     * </p>
     *
     * @param path
     *            Path to the parent container for the new data object
     * @param mediaType
     *            Declared media type of the data object
     * @param dataObject
     *            Raw content of the new data object
     */
    @PUT
    @Path("/{path:.+}")
    @Consumes(MediaTypes.DATA_OBJECT)
    @Produces(MediaTypes.DATA_OBJECT)
    public Response putDataObject(
            @Context HttpHeaders headers,
            @PathParam("path") String path,
            byte[] bytes) {

        System.out.println("putDataObject(): ");
        // print headers for debug
        for (String hdr : headers.getRequestHeaders().keySet()) {
            System.out.println(hdr + " - " + headers.getRequestHeader(hdr));
        }
        String inBuffer = new String(bytes);
        System.out.println("Path = " + path + "\n" + inBuffer);

        try {
            DataObject dObj = dataObjectDao.findByPath(path);
            if (dObj == null) {
                dObj = new DataObject();

                dObj.setObjectType("application/cdmi-object");
                // parse json
                dObj.fromJson(bytes, false);
                if (dObj.getValue() == null) {
                    dObj.setValue("== N/A ==");
                }
                dObj = dataObjectDao.createByPath(path, dObj);
                // return representation
                String respStr = dObj.toJson();
                return Response.ok(respStr).header(
                        "X-CDMI-Specification-Version", "1.0.2").build();
            }
            dObj.fromJson(bytes,false);
            return Response.ok().build();
        } catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).tag(
                  "Object PUT Error : " + ex.toString()).build();
        }
    }

    /**
     * <p>
     * [8.3] Create a new Data Object (Non-CDMI Content Type) and
     * [8.7] Update a Data Object (Non-CDMI Content Type)
     * </p>
     *
     * @param path
     *            Path to the new or existing data object
     * @param mediaType
     *            Declared media type of the data object
     * @param dataObject
     *            Raw content of the new data object
     */
    @PUT
    @Path("/{path:.+}")
    public Response putDataObject(
            @PathParam("path") String path,
            @HeaderParam("Content-Type") String contentType,
            byte[] bytes) {

        throw new UnsupportedOperationException(
                "PathResource.putDataObject(Non-CDMI Content Type");
    }

    /**
     * <p>
     * [9.10] Create a New Data Object (NON-CDMI Content Type)
     * </p>
     *
     * @param
     *      path Path to the new or existing data object
     * @param
     *      object value
     */
    @Path("/{path:.+}")
    @POST
    public Response postDataObject(
            @PathParam("path") String path,
            byte[] bytes) {

        String inBuffer = new String(bytes);
        System.out.println("Path = " + path + "\n" + inBuffer);

        boolean containerRequest = false;
        if (containerDao.isContainer(path)) {
            containerRequest = true;
        }

        try {
            String objectId = ObjectID.getObjectID(11);
            String objectPath = path + "/" + objectId;

            DataObject dObj = new DataObject();
            dObj.setObjectID(objectId);
            dObj.setObjectType(objectPath);
            dObj.setValue(inBuffer);

            System.out.println("objectId = " + objectId + " objecctPath = " +
                    objectPath);

            dObj = dataObjectDao.createByPath(objectPath, dObj);

            if (containerRequest) {
                return Response.ok().header("Location",
                        dObj.getObjectType()).build();
            }
            return Response.ok().build();
        } catch (Exception ex) {
            System.out.println(ex);
            ex.printStackTrace();
            return Response.status(Response.Status.BAD_REQUEST).
              tag("Object Creation Error : " + ex.toString()).build();
        }
    }


    /**
     * <p>
     * [9.3] Create a Container (Non-CDMI Content Type)
     * </p>
     *
     * <p>
     * FIXME - I do not see how to disambiguate this kind of call from
     *         creating a data object with a non-CDMI Content Type)
     */

}
