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
import java.net.URISyntaxException;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.dcache.cdmi.model.DcacheCapability;
import org.slf4j.LoggerFactory;

import org.snia.cdmiserver.dao.CapabilityDao;
import org.snia.cdmiserver.model.Capability;
import org.snia.cdmiserver.util.MediaTypes;

/**
 * <p>
 * Access to objects by path.
 * </p>
 */

@Path("/cdmi_capabilities{path:.*}")
public class DcacheCapabilityResource {

    /**
     * <p>
     * Injected information about the current request.
     * </p>
     */
    @Context
    UriInfo uriInfo;

    private CapabilityDao capabilityDao;

    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcacheCapabilityResource.class);

    /**
     * <p>
     * Injected {@link Capability} instance.
     * </p>
     * @param capabilityDao
     */
    public void setCapabilityDao(CapabilityDao capabilityDao) {
        this.capabilityDao = capabilityDao;
    }

    /**
     * <p>
     * [Chapter 12] Read a Capability Object (CDMI Content Type)
     * </p>
     *
     * @param path
     *            Path to the existing container
     * @return
     */
    @GET
    //@Produces(MediaTypes.CAPABILITY)
    public Response getCapabilityDao(@PathParam("path") String path) {
        System.out.println("In CapabilityResource.getCapabilityDao, path is: " + path);
        Capability capability = (DcacheCapability) capabilityDao.findByPath(path);
        try {
            if (capability instanceof DcacheCapability) {
                String respStr = ((DcacheCapability) capability).toJson();
                //Response.ResponseBuilder builder = Response.ok(capability).type(MediaTypes.CAPABILITY);
                Response.ResponseBuilder builder = Response.ok(new URI(path));
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                return builder.entity(respStr).build();
            } else {
                Response.ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
                builder.header("X-CDMI-Specification-Version", "1.0.2");
                return builder.entity("Capability Fetch Error").build();
            }
        } catch (URISyntaxException ex) {
            Response.ResponseBuilder builder = Response.status(Response.Status.NOT_FOUND);
            builder.header("X-CDMI-Specification-Version", "1.0.2");
            return builder.entity("Capability Fetch Error: " + ex.getMessage()).build();
        }
    }

}
