// $Id: PnfsGetCacheLocationsMessage.java,v 1.5 2007-05-24 13:51:05 tigran Exp $

package diskCacheV111.vehicles;
import  diskCacheV111.util.* ;

import java.util.*;

public class PnfsGetCacheLocationsMessage extends PnfsMessage {

    private List<String> _cacheLocations = null;

    private static final long serialVersionUID = 6603606352524630293L;

    public PnfsGetCacheLocationsMessage(){
	setReplyRequired(true);
    }

    public PnfsGetCacheLocationsMessage(String pnfsId){
	super(pnfsId);
	setReplyRequired(true);
    }
    public PnfsGetCacheLocationsMessage(PnfsId pnfsId){
	super(pnfsId);
	setReplyRequired(true);
    }

    public List<String> getCacheLocations(){
	return _cacheLocations;
    }

    public void setCacheLocations(List<String> cacheLocations){
    	_cacheLocations = cacheLocations;
    }
    public String toString(){
       StringBuffer sb = new StringBuffer() ;
       sb.append(getPnfsId()).append(";locs=") ;
       if( _cacheLocations != null )
         for( String location: _cacheLocations )
            sb.append(location).
               append(",") ;
       return sb.toString() ;
    }

    @Override
    public boolean invalidates(Message message)
    {
        return false;
    }

    @Override
    public boolean isSubsumedBy(Message message)
    {
        if (message.getClass().equals(PnfsGetCacheLocationsMessage.class)) {
            PnfsId pnfsId = getPnfsId();
            String path = getPnfsPath();
            PnfsGetCacheLocationsMessage msg =
                (PnfsGetCacheLocationsMessage) message;
            return
                (pnfsId == null || pnfsId.equals(msg.getPnfsId())) &&
                (path == null || path.equals(msg.getPnfsPath())) &&
                (getSubject().equals(msg.getSubject())) &&
                (getId() == msg.getId());
        }

        return false;
    }

    @Override
    public boolean isIdempotent()
    {
        return true;
    }
}