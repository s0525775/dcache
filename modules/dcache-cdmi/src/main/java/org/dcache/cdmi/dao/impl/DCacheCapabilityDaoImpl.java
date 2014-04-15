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

import diskCacheV111.util.PnfsHandler;
import javax.servlet.ServletContext;
import org.dcache.cells.CellStub;
import org.dcache.util.list.ListDirectoryHandler;
import org.slf4j.LoggerFactory;
import org.snia.cdmiserver.dao.CapabilityDao;
import org.snia.cdmiserver.model.Capability;
import org.snia.cdmiserver.util.ObjectID;
import org.springframework.web.context.ServletContextAware;

/**
 * <p>
 * Concrete implementation of {@link CapabilityObjectDao} using the local filesystem as the backing
 * store.
 * </p>
 */
public class DCacheCapabilityDaoImpl implements CapabilityDao, ServletContextAware
{

    //
    // Something important
    //
    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DCacheCapabilityDaoImpl.class);
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

    // -------------------------------------------------------------- Properties
    /**
     * <p>
     * Injected {@link CapabilityDao} instance.
     * </p>
     */
    private CapabilityDao capabilityDao;
    private final String ROOTobjectID = ObjectID.getObjectID(8);  //TODO? Might need to replaced by real ObjectID
    private final String CONTAINERobjectID = ObjectID.getObjectID(8);  //TODO? Might need to replaced by real ObjectID
    private final String DEFAULTobjectID = ObjectID.getObjectID(8);  //TODO? Might need to replaced by real ObjectID
    private final String OBJECTobjectID = ObjectID.getObjectID(8);  //TODO? Might need to replaced by real ObjectID

    public void setCapabilityDao(CapabilityDao capabilityDao)
    {
        this.capabilityDao = capabilityDao;
    }

    // ---------------------------------------------------- ContainerDao Methods
    @Override
    public Capability findByObjectId(String objectId)
    {
        throw new UnsupportedOperationException("DCacheCapabilityDaoImpl.findByObjectId()");
    }

    @Override
    public Capability findByPath(String path)
    {
        Capability capability = new Capability();

        _log.debug("In Capability.findByPath, path is: ");
        _log.debug(path);
        if (path.equals("container/")) {
            _log.debug("Container Capabilities");
            // Container Capabilities
            capability.getMetadata().put("cdmi_list_children", "true");
            capability.getMetadata().put("cdmi_read_metadata", "true");
            capability.getMetadata().put("cdmi_modify_metadata", "true");
            capability.getMetadata().put("cdmi_create_dataobject", "true");
            capability.getMetadata().put("cdmi_create_container", "true");
            capability.getChildren().add("default");
            capability.setObjectID(CONTAINERobjectID);
            capability.setObjectType("application/cdmi-capability");
            capability.setParentURI("cdmi_capabilities/");
            capability.setParentID(ROOTobjectID);
        } else if (path.equals("container/default/")) {
            _log.debug("Default Container Capabilities");
            capability.getMetadata().put("cdmi_list_children", "true");
            capability.getMetadata().put("cdmi_read_metadata", "true");
            capability.getMetadata().put("cdmi_modify_metadata", "true");
            capability.getMetadata().put("cdmi_create_dataobject", "true");
            capability.getMetadata().put("cdmi_post_dataobject", "true");
            capability.getMetadata().put("cdmi_create_container", "true");
            capability.setObjectID(DEFAULTobjectID);
            capability.setObjectType("application/cdmi-capability");
            capability.setParentURI("cdmi_capabilities/container");
            capability.setParentID(CONTAINERobjectID);

        } else if (path.equals("dataobject/")) {
            // Data Object Capabilities
            _log.debug("Data Object Capabilities");
            capability.getMetadata().put("cdmi_read_value", "true");
            capability.getMetadata().put("cdmi_read_metadata", "true");
            capability.getMetadata().put("cdmi_modify_metadata", "true");
            capability.getMetadata().put("cdmi_modify_value", "true");
            capability.getMetadata().put("cdmi_delete_dataobject", "true");
            capability.setObjectID(OBJECTobjectID);
            capability.setObjectType("application/cdmi-capability");
            capability.setParentURI("cdmi_capabilities/");
            capability.setParentID(ROOTobjectID);
        } else {
            // System Capabilities
            _log.debug("System Capabilities");
            capability.getMetadata().put("domains", "false");
            capability.getMetadata().put("cdmi_export_occi_iscsi", "true");
            capability.getMetadata().put("cdmi_metadata_maxitems", "1024");
            capability.getMetadata().put("cdmi_metadata_maxsize", "4096");
            // Data System Metadata Support - Start
            capability.getMetadata().put("cdmi_assignedsize", "false");
            capability.getMetadata().put("cdmi_data_redundancy", "");
            capability.getMetadata().put("cdmi_data_dispersion", "false");
            capability.getMetadata().put("cdmi_data_retention", "false");
            capability.getMetadata().put("cdmi_data_autodelete", "false");
            capability.getMetadata().put("cdmi_data_holds", "false");
            capability.getMetadata().put("cdmi_encryption", "{}");
            capability.getMetadata().put("cdmi_geographic_placement", "false");
            capability.getMetadata().put("cdmi_immediate_redundancy", "");
            capability.getMetadata().put("cdmi_infrastructure_redundancy", "");
            capability.getMetadata().put("cdmi_latency", "false");
            capability.getMetadata().put("cdmi_RPO", "false");
            capability.getMetadata().put("cdmi_RTO", "false");
            capability.getMetadata().put("cdmi_sanitization_method", "{}");
            capability.getMetadata().put("cdmi_throughput", "false");
            capability.getMetadata().put("cdmi_value_hash", "{}");
            // Data System Metadata Support - End
            capability.getChildren().add("container");
            capability.getChildren().add("dataobject");
            capability.setObjectID(ROOTobjectID);
            capability.setObjectType("application/cdmi-capability");
            capability.setParentURI("/");
            capability.setParentID(ROOTobjectID);
        }
        return (capability);
    }

    @Override
    public void setServletContext(ServletContext sContext) {
        _log.debug("Init DCacheCapabilityDaoImpl...");
        this.servletContext = sContext;
        this.pnfsStub = getCellStubAttribute();
        this.pnfsHandler = new PnfsHandler(pnfsStub);
        //this.listDirectoryHandler = new ListDirectoryHandler(pnfsHandler); //does not work, tested 100 times
        this.listDirectoryHandler = getListDirAttribute(); //it only works in this way, tested 100 times
        this.poolStub = getPoolAttribute();
        this.poolMgrStub = getPoolMgrAttribute();
        this.billingStub = getBillingAttribute();
    }

    private CellStub getCellStubAttribute()
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

    private ListDirectoryHandler getListDirAttribute()
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

    private CellStub getPoolAttribute()
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

    private CellStub getPoolMgrAttribute()
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

    private CellStub getBillingAttribute()
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

}
