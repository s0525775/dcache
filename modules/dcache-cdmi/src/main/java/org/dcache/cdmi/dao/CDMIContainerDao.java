/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.dcache.cdmi.dao;

import org.dcache.cdmi.model.CDMIContainer;
import org.snia.cdmiserver.dao.ContainerDao;

/**
 *
 * @author Jana
 */

/**
* <p>
* DAO for manipulating CDMI <em>Container</em> instances.
* </p>
*/
public interface CDMIContainerDao extends ContainerDao {

    /**
    * <p>
    * Create a container at the specified path. All intermediate containers must already exist.
    * </p>
    *
    * @param path
    * Path to the new {@link CDMIContainer}
         * @param containerRequest
         * @return
    * @exception IllegalArgumentException
    * if an intermediate container does not exist
    */
    public CDMIContainer createByPath(String path, CDMIContainer containerRequest);

    /**
    * <p>
    * Find and return a {@link CDMIContainer} by object id, if any; otherwise, return <code>null</code>
    * .
    * </p>
    *
    * @param objectId
    * Object ID of the requested {@link CDMIContainer}
         * @return
    */
    @Override
    public CDMIContainer findByObjectId(String objectId);

    /**
    * <p>
    * Find and return a {@link CDMIContainer} by path, if any; otherwise, return <code>null</code>.
    * </p>
    *
    * @param path
    * Path to the requested {@link CDMIContainer}
         * @return
    * @exception IllegalArgumentException
    * if the specified path identifies a data object instead of a container
    */
    @Override
    public CDMIContainer findByPath(String path);

}
