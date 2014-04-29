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

import com.google.common.collect.ImmutableMap;
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
public class DcacheCapabilityDao implements CapabilityDao
{

    private final static org.slf4j.Logger _log = LoggerFactory.getLogger(DcacheCapabilityDao.class);

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
     * Injected {@link CapabilityDao} instance.
     * </p>
     */
    private final String ROOTobjectID = ObjectID.getObjectID(8);  //TODO? Might need to replaced by real ObjectID
    private final String CONTAINERobjectID = ObjectID.getObjectID(8);  //TODO? Might need to replaced by real ObjectID
    private final String DEFAULTobjectID = ObjectID.getObjectID(8);  //TODO? Might need to replaced by real ObjectID
    private final String OBJECTobjectID = ObjectID.getObjectID(8);  //TODO? Might need to replaced by real ObjectID

    @Override
    public Capability findByObjectId(String objectId)
    {
        throw new UnsupportedOperationException("DCacheCapabilityDaoImpl.findByObjectId()");
    }

    @Override
    public Capability findByPath(String path)
    {
        Capability capability = new Capability();

        _log.trace("In Capability.findByPath, path={" + path + "}");
        switch (path) {
            case "container/":
                _log.trace("Container Capabilities");
                // Container Capabilities
                capability.getMetadata().putAll(CONTAINER_METADATA);
                capability.getChildren().add("default");
                capability.setObjectID(CONTAINERobjectID);
                capability.setParentURI("cdmi_capabilities/");
                capability.setParentID(ROOTobjectID);
                break;
            case "container/default/":
                _log.trace("Default Container Capabilities");
                capability.getMetadata().putAll(DEFAULT_CONTAINER_METADATA);
                capability.setObjectID(DEFAULTobjectID);
                capability.setParentURI("cdmi_capabilities/container");
                capability.setParentID(CONTAINERobjectID);
                break;
            case "dataobject/":
                // Data Object Capabilities
                _log.trace("Data Object Capabilities");
                capability.getMetadata().putAll(DATAOBJECT_METADATA);
                capability.setObjectID(OBJECTobjectID);
                capability.setParentURI("cdmi_capabilities/");
                capability.setParentID(ROOTobjectID);
                break;
            default:
                // System Capabilities
                _log.trace("System Capabilities");
                capability.getMetadata().putAll(DEFAULT_METADATA);
                capability.getChildren().add("container");
                capability.getChildren().add("dataobject");
                capability.setObjectID(ROOTobjectID);
                capability.setParentURI("/");
                capability.setParentID(ROOTobjectID);
                break;
        }
        capability.setObjectType("application/cdmi-capability");
        return (capability);
    }

}
