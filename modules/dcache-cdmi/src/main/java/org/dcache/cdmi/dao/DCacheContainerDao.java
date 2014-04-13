package org.dcache.cdmi.dao;

import org.dcache.cdmi.model.DCacheContainer;
import org.snia.cdmiserver.dao.ContainerDao;
import org.snia.cdmiserver.model.Container;

/**
* <p>
* DAO for manipulating CDMI <em>Container</em> instances.
* </p>
*/
public interface DCacheContainerDao extends ContainerDao
{

    /**
    * <p>
    * Create a container at the specified path. All intermediate containers must already exist.
    * </p>
    *
    * @param path
    * Path to the new {@link DCacheContainer}
         * @param containerRequest
         * @return
    * @exception IllegalArgumentException
    * if an intermediate container does not exist
    */
    public DCacheContainer createByPath(String path, Container containerRequest);

    /**
    * <p>
    * Find and return a {@link DCacheContainer} by object id, if any; otherwise, return <code>null</code>
    * .
    * </p>
    *
    * @param objectId
    * Object ID of the requested {@link DCacheContainer}
         * @return
    */
    @Override
    public DCacheContainer findByObjectId(String objectId);

    /**
    * <p>
    * Find and return a {@link DCacheContainer} by path, if any; otherwise, return <code>null</code>.
    * </p>
    *
    * @param path
    * Path to the requested {@link DCacheContainer}
         * @return
    * @exception IllegalArgumentException
    * if the specified path identifies a data object instead of a container
    */
    @Override
    public DCacheContainer findByPath(String path);

}
