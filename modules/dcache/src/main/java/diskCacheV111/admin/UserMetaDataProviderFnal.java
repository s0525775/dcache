package diskCacheV111.admin ;

import org.dcache.auth.KAuthFile;
import java.util.* ;
import java.io.* ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import diskCacheV111.util.*;
import org.dcache.auth.UserAuthBase;
import org.dcache.auth.UserAuthRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author : Patrick Fuhrmann, Vladimir Podstavkov
 * Based on the UserMetaDataProviderExample
 *
 */
public class UserMetaDataProviderFnal implements UserMetaDataProvider {

    private final static Logger _log =
        LoggerFactory.getLogger(UserMetaDataProviderFnal.class);

    private final CellAdapter _cell    ;
    private final Map<String,Object>  _context;
    private final Args        _args    ;
    private final String      _ourName ;

    private int     _requestCount      = 0 ;
    private final Map<String, Integer> _userStatistics =
        CollectionFactory.newHashMap();

    //generalized kpwd file path used by all flavors
    private String _kpwdFilePath = null;

    /**
     * We are assumed to provide the following constructor signature.
     */
    public UserMetaDataProviderFnal(CellAdapter cell) {

        _cell    =  cell ;
        _context = _cell.getDomainContext() ;
        _args    = _cell.getArgs() ;
        _ourName = this.getClass().getName() ;
        //
        //
        //  get some information from the
        //  command line or the domain context.
        //
        _kpwdFilePath = (String)_context.get("kpwd-file") ;
        _kpwdFilePath = _kpwdFilePath == null ? _args.getOpt("kpwd-file") : _kpwdFilePath;

        if (_kpwdFilePath == null)
            throw new IllegalArgumentException(_ourName+" : -kpwd-file not specified");
    }

    /**
     * just for the fun of it
     */
    public String hh_ls = "" ;

    public String ac_ls( Args args ) {
        StringBuffer sb = new StringBuffer() ;
        Iterator i = _userStatistics.entrySet().iterator() ;
        while ( i.hasNext() ) {
            Map.Entry entry = (Map.Entry)i.next() ;
            sb.append(entry.getKey().toString()).
                append("  ->  ").
                append(entry.getValue().toString()).
                append("\n") ;
        }
        return sb.toString();
    }

    private void updateStatistics( String userName ) {
        Integer count = (Integer)_userStatistics.get(userName);
        int c = count == null ? 0 : count.intValue() ;
        _userStatistics.put( userName , Integer.valueOf( c+1 ) ) ;
        _requestCount++ ;
    }

    /**
     * and of course the interface definition
     */
    public synchronized Map getUserMetaData( String userName, String userRole, List attributes )
        throws Exception {

        //
        // 'attributes' is a list of keys somebody (door)
        // needs from us. We are assumed to prepare
        // a map containing the 'key' and the
        // corresponding values.
        // we should at least be prepared to know the
        // 'uid','gid' of the user.
        // If we are not sure about the user, we should
        // throw an exception rather returning an empty
        // map.
        //
        updateStatistics( userName ) ;
        //
        // get the information for the user
        //
        Map<String,String> result = getUserMD(userName, userRole) ;
        //
        // check for minimum requirments
        //
        if ( ( result.get("uid") == null ) ||
             ( result.get("gid") == null ) ||
             ( result.get("home") == null )  )
            throw new IllegalArgumentException(_ourName+" : insufficient info for user : "+userName+"->"+userRole);

        return result;

    }


    private Map<String,String> getUserMD(String userPrincipal, String userRole)
    {
        KAuthFile authf;
        UserAuthBase pwdRecord = null;
        Map<String, String> answer = CollectionFactory.newHashMap();
        int uid, gid;
        String home;

        try {
            authf = new KAuthFile(_kpwdFilePath);
        }
        catch ( Exception e ) {
            _log.warn("User authentication file not found: " + e);
            return answer;
        }
        if (userRole.startsWith("UNSPECIFIED")) {

            userRole = authf.getIdMapping(userPrincipal);
            _log.warn("userRole="+userRole);

            if(userRole == null) {
                _log.warn("User " + userPrincipal + " not found.");
                return answer;
            }
        }
        pwdRecord = authf.getUserRecord(userRole);
        if( pwdRecord == null ) {
            _log.warn("User " + userRole + " not found.");
            return answer;
        }

        if( !((UserAuthRecord)pwdRecord).hasSecureIdentity(userPrincipal) ) {
            pwdRecord = null;
            _log.warn(userPrincipal+": Permission denied");
            return answer;
        }
        uid  = pwdRecord.UID;
        gid  = pwdRecord.GID;
        home = pwdRecord.Home;

        answer.put("uid", String.valueOf(uid));
        answer.put("gid", String.valueOf(gid));
        answer.put("home", home);

        _log.info("User "+userRole+" logged in");
        return answer;
    }


    /**
     * and of course the interface definition
     */
    public String toString() {
        return "rc="+_requestCount;
    }

}