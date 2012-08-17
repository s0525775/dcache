package dmg.cells.services.login ;

import dmg.util.* ;
import java.util.* ;
import java.io.* ; 


public class UserPrivileges {
   private Hashtable _allowed = new Hashtable() ;
   private Hashtable _denied  = new Hashtable() ;
   private String    _userName = "unknown" ;
   private boolean   _faked    = false ;
   UserPrivileges(){}
   UserPrivileges( String userName ){ _userName = userName ; _faked = true ; }
   UserPrivileges( String userName ,
                   String [] allowedList ,
                   String [] deniedList     ){
   
        _userName = userName ;
        for( int i = 0 ; i < allowedList.length ; i++ )
            _allowed.put( allowedList[i] , allowedList[i] ) ;
        for( int i = 0 ; i < deniedList.length ; i++ ){
            _denied.put( deniedList[i] , deniedList[i] ) ;
            _allowed.remove( deniedList[i] ) ;
        }
            
   }
   public String getUserName(){ return _userName ; }
   void mergeHorizontal( UserPrivileges right ){
       String attr = null ;
       
       Enumeration e = right._denied.keys() ;
       for( ; e.hasMoreElements() ; ){
          _denied.put( attr = (String)e.nextElement() , attr ) ;
       }
       e = right._allowed.keys() ;
       for( ; e.hasMoreElements() ; ){
          _allowed.put( attr = (String)e.nextElement() , attr  ) ;
          _denied.remove( attr ) ;
       }
   }
   void mergeVertical( UserPrivileges upper ){
       String attr = null ;
       
       Enumeration e = upper._allowed.keys() ;
       for( ; e.hasMoreElements() ; ){
          if( _denied.get( (attr = (String)e.nextElement() ) ) == null )
            _allowed.put( attr , attr ) ;
       }
       e = upper._denied.keys() ;
       for( ; e.hasMoreElements() ; ){
          if( _allowed.get((attr=(String)e.nextElement())) == null )
             _denied.put( attr , attr ) ;
       }
   }
   public boolean isAllowed( String check ){
    if( _faked )return false ;
    if( _userName.equals("root") )return true ;
    try{
       if( _denied.get( check ) != null )return false ;
       if( _allowed.get( check ) != null )return true ;
       String base = null ;
       int last = check.lastIndexOf(':') ;
       if( last < 0 ){
         base  = "" ;
       }else{
         base  = check.substring(0,last)+":" ;
         check = check.substring(last+1) ;
       }
       if( check.length() < 1 )return false ;
       
       StringTokenizer st = new StringTokenizer(check,".") ;
       String [] tokens = new String[st.countTokens()] ;
       
       for( int i = 0  ; i < tokens.length ; i++ )
          tokens[i] = st.nextToken() ;
        
       for( int i = tokens.length  ; i > 0 ; i-- ){
          StringBuffer sb = new StringBuffer() ;
          sb.append( base ) ;
          for( int j = 0 ; j < (i-1) ; j++ )
             sb.append( tokens[j] ).append(".") ;
          sb.append( "*" ) ;
          String x = sb.toString() ;
          if( _denied.get( x ) != null )return false ;
          if( _allowed.get( x ) != null )return true ;
       }
       return false ;
      }catch( Exception ee ){
         ee.printStackTrace() ;
         return false ;
      }
   }
   public String toString(){
       if( _faked )return "UserPrivileges for "+_userName+" faked" ;
       StringBuffer sb = new StringBuffer() ;
       String x = "         " ;
       int dx = 20 ;
       int m = Math.min( _allowed.size() , _denied.size() ) ;
       Enumeration a = _allowed.keys() ;
       Enumeration d = _denied.keys() ;
       for( int i = 0 ; i < m ; i ++ )
          sb.append(x).
             append(Formats.field((String)a.nextElement(),dx)).
             append(Formats.field((String)d.nextElement(),dx)).
             append("\n") ;
       if( _allowed.size() > m )
          while( a.hasMoreElements() )
             sb.append(x).
                append(Formats.field((String)a.nextElement(),dx)).
                append(Formats.field("",20)).
                append("\n") ;
       if( _denied.size() > m )
          while( d.hasMoreElements() )
             sb.append(x).
                append(Formats.field("",20)).
                append(Formats.field((String)d.nextElement(),dx)).
                append("\n") ;
      
       return sb.toString() ;
   }

}