package org.dcache.cdmi.dao;

import org.dcache.cdmi.model.DCacheDataObject;
import org.snia.cdmiserver.dao.DataObjectDao;
import org.snia.cdmiserver.model.DataObject;

/**
* <p>
* DAO for manipulating CDMI <em>DataObject</em> instances.
* </p>
*/
public interface DCacheDataObjectDao extends DataObjectDao
{

    /**
    * <p>
    * Create a data object at the specified path. All intermediate containers must already exist.
    * </p>
    *
    * @param path
    * Path to the new {@link DCacheDataObject}
         * @param dObj
         * @return
    * @exception IllegalArgumentException
    * if an intermediate container does not exist
         * @throws java.lang.Exception
    */
    @Override
    public DCacheDataObject createByPath(String path, DataObject dObj) throws Exception;

    @Override
    public DCacheDataObject createById(String objectId, DataObject dObj);

    /**
    * <p>
    * Find and return a {@link DCacheDataObject} by object id, if any; otherwise, return
    * <code>null</code>.
    * </p>
    *
    * @param objectId
    * Object ID of the requested {@link DCacheDataObject}
         * @return
    */
    @Override
    public DCacheDataObject findByObjectId(String objectId);

    /**
    * <p>
    * Find and return a {@link DCacheDataObject} by path, if any; otherwise, return <code>null</code>.
    * </p>
    *
    * @param path
    * Path to the requested {@link DCacheDataObject}
         * @return
    */
    @Override
    public DCacheDataObject findByPath(String path);

}