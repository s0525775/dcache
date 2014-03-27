/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.cdmi.dao;

import org.dcache.cdmi.model.CDMIDataObject;
import org.snia.cdmiserver.dao.DataObjectDao;

/**
 *
 * @author Jana
 */

/**
* <p>
* DAO for manipulating CDMI <em>DataObject</em> instances.
* </p>
*/
public interface CDMIDataObjectDao extends DataObjectDao
{

    /**
    * <p>
    * Create a data object at the specified path. All intermediate containers must already exist.
    * </p>
    *
    * @param path
    * Path to the new {@link CDMIDataObject}
         * @param dObj
         * @return
    * @exception IllegalArgumentException
    * if an intermediate container does not exist
         * @throws java.lang.Exception
    */
    public CDMIDataObject createByPath(String path, CDMIDataObject dObj) throws Exception;

    public CDMIDataObject createById(String objectId, CDMIDataObject dObj);

    /**
    * <p>
    * Find and return a {@link CDMIDataObject} by object id, if any; otherwise, return
    * <code>null</code>.
    * </p>
    *
    * @param objectId
    * Object ID of the requested {@link CDMIDataObject}
         * @return
    */
    @Override
    public CDMIDataObject findByObjectId(String objectId);

    /**
    * <p>
    * Find and return a {@link CDMIDataObject} by path, if any; otherwise, return <code>null</code>.
    * </p>
    *
    * @param path
    * Path to the requested {@link CDMIDataObject}
         * @return
    */
    @Override
    public CDMIDataObject findByPath(String path);

}