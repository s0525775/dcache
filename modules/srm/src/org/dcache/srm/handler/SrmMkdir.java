//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 11/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

/*
 * SrmMkdir
 *
 * Created on 11/05
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.v2_2.SrmMkdirRequest;
import org.dcache.srm.v2_2.SrmMkdirResponse;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMInvalidPathException;
import org.apache.log4j.Logger;
import org.apache.axis.types.URI.MalformedURIException;

/**
 *
 * @author  litvinse
 */

public class SrmMkdir {
        private static final Logger logger = 
                Logger.getLogger(SrmMkdir.class.getName());
	private final static String SFN_STRING="?SFN=";
	AbstractStorageElement storage;
	SrmMkdirRequest        request;
	SrmMkdirResponse       response;
	SRMUser            user;
	
	public SrmMkdir(SRMUser user,
			RequestCredential credential,
			SrmMkdirRequest request,
			AbstractStorageElement storage,
			org.dcache.srm.SRM srm,
			String client_host ) {
		this.request = request;
		this.user = user;
		this.storage = storage;
	}
	
	public SrmMkdirResponse getResponse() {
		if(response != null ) return response;
		try {
			response = srmMkdir();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMException srme) {
            logger.error(srme);
            response = getFailedResponse(srme.toString());
        }
		return response;
	}
	
	public static final SrmMkdirResponse getFailedResponse(String error) {
		return getFailedResponse(error,null);
	}
	
	public static final SrmMkdirResponse getFailedResponse(String error,TStatusCode statusCode) {
		if(statusCode == null) {
			statusCode =TStatusCode.SRM_FAILURE;
		}
		TReturnStatus status = new TReturnStatus();
		status.setStatusCode(statusCode);
		status.setExplanation(error);
		SrmMkdirResponse response = new SrmMkdirResponse();
		response.setReturnStatus(status);
		return response;
	}
	
	
	/**
	 * implementation of srm mkdir
     */
	
	public SrmMkdirResponse srmMkdir() throws SRMException,
            MalformedURIException {
		SrmMkdirResponse response  = new SrmMkdirResponse();
		TReturnStatus returnStatus = new TReturnStatus();
		returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
		response.setReturnStatus(returnStatus);
		if(request==null) {
		return getFailedResponse(" null request passed to SrmRm()");
		}
		org.apache.axis.types.URI surl = request.getSURL();
		int port    = surl.getPort();
		String host = surl.getHost();
		String path = surl.getPath(true,true);
		int indx    = path.indexOf(SFN_STRING);
		if ( indx != -1 ) {
			path=path.substring(indx+SFN_STRING.length());
		}
		try {
			storage.createDirectory(user,path);
		} 
        catch (SRMDuplicationException srmde) {
            logger.debug("srmMkdir duplication : "+srmde.toString());
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_DUPLICATION_ERROR);
			response.getReturnStatus().setExplanation(surl+" : "+srmde.getMessage());
			return response;
                   
        }
        catch (SRMAuthorizationException srmae) {
            logger.debug("srmMkdir authorization exception : "+srmae.toString());
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
			response.getReturnStatus().setExplanation(surl+" : "+srmae.getMessage());
			return response;
                   
        }
        catch (SRMInvalidPathException srmipe) {
            logger.debug("srmMkdir invalid pathh : "+srmipe.toString());
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_INVALID_PATH);
			response.getReturnStatus().setExplanation(surl+" : "+srmipe.getMessage());
			return response;
                   
        }
		catch (SRMException srme) {
            logger.debug("srmMkdir error ",srme);        
			response.getReturnStatus().setStatusCode(TStatusCode.SRM_FAILURE);
			response.getReturnStatus().setExplanation(surl+" "+srme.getMessage());
			return response;
		}
		response.getReturnStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
		response.getReturnStatus().setExplanation("success");
		return response;
	}
}