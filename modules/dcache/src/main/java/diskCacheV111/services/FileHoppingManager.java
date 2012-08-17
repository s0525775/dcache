 // $Id: FileHoppingManager.java,v 1.3 2006-04-21 11:21:33 patrick Exp $Cg

package  diskCacheV111.services ;

import java.util.* ;
import java.text.* ;
import java.io.* ;
import java.util.regex.Pattern ;

import dmg.cells.nucleus.* ;
import dmg.util.* ;


import diskCacheV111.poolManager.* ;
import diskCacheV111.vehicles.* ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
  *  @Author: Patrick Fuhrmann
  *
  *  The FileHoppingManager receives PoolMgrReplicateFileMsg messages from
  *  write pools and, depending
  *
  */
public class FileHoppingManager extends CellAdapter {

   private final static Logger _log =
       LoggerFactory.getLogger(FileHoppingManager.class);

   private CellNucleus _nucleus       = null ;
   private Args        _args          = null ;
   private Map         _map           = new TreeMap() ;
   private final Object      _mapLock       = new Object() ;
   private int         _totalRequests = 0 ;
   private File        _configFile    = null ;

   private CellPath    _defaultDestinationPath = new CellPath("PoolManager");

   public  CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.3 $" ); }

   public FileHoppingManager( String name , String args )throws Exception {

      super( name , FileHoppingManager.class.getName(), args , false );

      _args    = getArgs() ;
      _nucleus = getNucleus() ;

      try{

         if( _args.argc() < 1 )
            throw new
            IllegalArgumentException(
               "Usage : <configFileName> OPTIONS ");

         _configFile = new File( _args.argv(0) ) ;

         if( ! _configFile.exists() ){
            File configDir = _configFile.getParentFile() ;
            if( ( configDir == null ) || ! configDir.exists() )
              throw new
              IllegalArgumentException("Config directory doesn't exit : "+configDir);

            try{
               if( ! _configFile.createNewFile() )
                  throw new
                  IllegalArgumentException("Couldn't create config file : "+_configFile);
            }catch(Exception ee){
               throw new
                  IllegalArgumentException("Couldn't create config file : "+_configFile+" : "+ee.getMessage());
            }
         }

      }catch(Exception ee){
         ee.printStackTrace();
         start();
         kill() ;
         throw ee ;
      }


      runSetupFile( _configFile ) ;

      _nucleus.export();

      start() ;

   }
   private void runSetupFile( File setupFile ) throws Exception {

      if( ! setupFile.exists() )
         throw new
         IllegalArgumentException( "Setup File not found : "+setupFile ) ;

      BufferedReader reader = new BufferedReader( new FileReader( setupFile ) ) ;
      try{


         String line = null ;
         while( ( line = reader.readLine() ) != null ){
            if( line.length() == 0 )continue ;
            if( line.charAt(0) == '#' )continue ;
            try{
               _log.info( "Executing : "+line ) ;
               String answer = command( line ) ;
               if( answer.length() > 0 )_log.info( "Answer    : "+answer ) ;
            }catch( Exception ee ){
               _log.warn("Exception : "+ee.toString() ) ;
            }
         }
      }finally{
         try{ reader.close() ; }catch(Exception ee){}
      }

   }
   private void dumpSetup() throws Exception {

       File setupFile = _configFile.getCanonicalFile() ;
       File tmpFile   = new File( setupFile.getParent() , "."+setupFile.getName() ) ;

       PrintWriter writer =
          new PrintWriter( new FileWriter( tmpFile ) ) ;

       try{
          writer.print( "#\n# Setup of " ) ;
          writer.print(_nucleus.getCellName() ) ;
          writer.print(" (") ;
          writer.print(this.getClass().getName()) ;
          writer.print(") at ") ;
          writer.println( new Date().toString() ) ;
          writer.println( "#") ;

          synchronized( _mapLock ){
              for( Iterator i = _map.values().iterator() ; i.hasNext() ; ){
                  Entry e = (Entry)i.next() ;
                  writer.println( e.toCommandString() ) ;
              }
          }

          writer.println( "#" ) ;

       }catch(Exception ee){
          tmpFile.delete() ;
          throw ee ;
       }finally{
          try{ writer.close() ; }catch(Exception eee ){}
       }
       if( ! tmpFile.renameTo( setupFile ) ){

          tmpFile.delete() ;

          throw new
          IllegalArgumentException( "Rename failed : "+_configFile ) ;

       }
       return ;
   }
   public void getInfo( PrintWriter pw ){
       pw.println("     Cell Name  : "+getCellName());
       pw.println("    Cell Class  : "+this.getClass().getName() );
       pw.println("       Version  : $Id: FileHoppingManager.java,v 1.3 2006-04-21 11:21:33 patrick Exp $");
       pw.println("  Total Requsts : "+_totalRequests ) ;
       pw.println("Number of Rules : "+_map.size());
   }
   private class Entry {

      private String  _name       = null ;
      private boolean _retry      = false ;
      private boolean _continue   = false ;

      private String  _patternStr = null ;
      private Pattern _pattern    = null ;

      private int     _status     = 0 ;
      private String  _statusStr  = null ;

      private String   _dest      = null ;
      private CellPath _path      = null ;

      private String   _source    = "write" ;

      private ProtocolInfo _info  = null ;
      private String   _hostName  = null ;
      private String   _protType  = null ;
      private int      _protMinor = 0 ;
      private int      _protMajor = 0 ;

      private int      _hit       = 0 ;

      private Entry( String name ,
                     String patternStr ,
                     String modeString
                    ){

         _name       = name ;

         _patternStr = patternStr ;
         _pattern    = Pattern.compile(patternStr) ;


         _statusStr  = modeString == null ? "keep" : modeString ;
         _status     = getModeByString( _statusStr ) ;

      }
      public String toCommandString(){
         StringBuffer sb = new StringBuffer() ;
         sb.append("define hop ").append(_name).append(" \"").
            append(_patternStr).append("\" ").append(_statusStr);

         if( _info != null ){
            sb.append(" -host=").append(_hostName).
               append(" -protType=").append(_protType).
               append(" -protMinor=").append(_protMinor).
               append(" -protMajor=").append(_protMajor) ;
         }

         if( _dest != null )
           sb.append(" -destination=").append(_dest);

         sb.append(" -source=").append(_source) ;

         if( _continue )sb.append(" -continue");
         if( _retry )sb.append(" -retry");

         return sb.toString();
      }
      public void setContinue( boolean isContinue ){
        _continue = isContinue ;
      }
      public void setRetry( boolean isRetry ){
        _retry = isRetry ;
      }
      public void setSource( String source ){
        _source = source ;
      }
      public void setDestination( String destination ){
         if( destination == null )return ;
         _dest       = destination ;
         _path       = new CellPath( _dest ) ;
      }
      public int getModeByString( String modeStr ){

          int mode = Pool2PoolTransferMsg.UNDETERMINED ;

          if( modeStr == null ){
             mode = Pool2PoolTransferMsg.UNDETERMINED ;
          }else if( modeStr.equals("precious") ){
             mode = Pool2PoolTransferMsg.PRECIOUS ;
          }else if( modeStr.equals("cached") ){
             mode = Pool2PoolTransferMsg.CACHED ;
          }else if( modeStr.equals("keep") ){
             mode = Pool2PoolTransferMsg.UNDETERMINED ;
          }else{
             throw new
             IllegalArgumentException("Mode string : precious|cached|keep");
          }
          return mode ;
      }

      public void setProtocolInfo( String hostName , String protType , String protMajor , String protMinor ){

          if( ( hostName  != null ) || ( protType  != null ) ||
              ( protMinor != null ) || ( protMajor != null )    ){

              _hostName  = hostName  == null ? "localhost" : hostName ;
              _protType  = protType  == null ? "DCap" : protType ;
              _protMinor = protMinor == null ? 0 : Integer.parseInt( protMinor ) ;
              _protMajor = protMajor == null ? 3 : Integer.parseInt( protMajor ) ;

              _info = new DCapProtocolInfo(
                                 _protType, _protMajor, _protMajor,
                                 hostName ,
                                 0
                        ) ;

          }
      }
      public ProtocolInfo getProtocolInfo(){ return _info ; }
      public String toString(){
         return _name+"=\""+_patternStr+"\";st="+_status+
                      ";dest="+_path+
                      ";source="+_source+
                      ";info="+(_info==null?"None":_info.toString());
      }
   }


   public String fh_define_hop =
      "define hop OPTIONS <name> <pattern> precious|cached|keep\n"+
      "    OPTIONS\n"+
      "       -destination=<cellDestination> # default : PoolManager\n"+
      "       -overwrite\n"+
      "       -retry\n"+
      "       -continue\n"+
      "       -source=write|restore|*   #  !!!! for experts only"+
      "      StorageInfoOptions\n"+
      "         -host=<destinationHostIp>\n"+
      "         -protType=dCap|ftp...\n"+
      "         -protMinor=<minorProtocolVersion>\n"+
      "         -protMajor=<majorProtocolVersion>\n" ;

   public String hh_define_hop =
      "OPTONS <name> <pattern> precious|cached|keep # see 'help define hop'" ;
   public String ac_define_hop_$_3( Args args ){

      String name        = args.argv(0) ;
      String patternStr  = args.argv(1) ;
      String modeStr     = args.argv(2) ;

      String destination = args.getOpt("destination") ;

      boolean overwrite  = args.hasOption("overwrite") ;

      String hostName    = args.getOpt("host") ;
      String protocol    = args.getOpt("protType") ;
      String protMinor   = args.getOpt("protMinor") ;
      String protMajor   = args.getOpt("protMajor") ;

      String source      = args.getOpt("source") ;

      Entry entry = (Entry)_map.get( name ) ;
      if( ( entry != null ) && ! overwrite )
         throw new
         IllegalArgumentException("Entry already exists : "+name ) ;

      entry = new Entry( name , patternStr , modeStr ) ;

      entry.setProtocolInfo( hostName , protocol , protMajor , protMinor ) ;

      entry.setDestination( destination ) ;

      entry.setRetry( args.hasOption("retry") ) ;
      entry.setContinue( args.hasOption("continue") ) ;

      if( source != null )entry.setSource(source) ;

      synchronized( _mapLock ){
            _map.put( name , entry ) ;
      }
      return "" ;
   }
   public String hh_rename_hop = "<oldName> <newName>" ;
   public String ac_rename_hop_$_2( Args args ){
      String oldName = args.argv(0) ;
      String newName = args.argv(1) ;

      synchronized( _mapLock ){
         Entry oldEntry = (Entry)_map.remove(oldName) ;
         if( oldEntry == null )
            throw new
            IllegalArgumentException("currentName not found : "+oldName );

         Entry newEntry = (Entry)_map.get(newName) ;
         if( newEntry != null )
            throw new
            IllegalArgumentException("newName already exists: "+newName );

         oldEntry._name = newName ;
         _map.put( newName , oldEntry ) ;

      }
      return "" ;
   }
   public String hh_undefine_hop = "<name>" ;
   public String ac_undefine_hop_$_1( Args args ){

      String name = args.argv(0) ;
      synchronized( _mapLock ){
          _map.remove( name ) ;
      }
      return "" ;
   }
    public void messageToForward(  CellMessage cellMessage ){
        _log.info("Message to forward : "+cellMessage.getMessageObject().getClass().getName());
        // super.messageToForward(cellMessage);
    }
   public void replicateReplyArrived( CellMessage message , PoolMgrReplicateFileMsg replicate ){
      _log.info("replicateReplyArrived : "+message);
   }
   public void messageArrived( CellMessage message ){

      Object   request     = message.getMessageObject() ;

      _log.info("messageArrived : "+request+" : "+message);
      if( request instanceof PoolMgrReplicateFileMsg ){

         PoolMgrReplicateFileMsg replicate = (PoolMgrReplicateFileMsg)request ;
         if( replicate.isReply() ){
            replicateReplyArrived( message , replicate ) ;
            return ;
         }
         replicate.setReplyRequired(true);

         StorageInfo storageInfo = replicate.getStorageInfo() ;
         if( storageInfo == null )return ;

         _totalRequests ++ ;

         String storageClass      = storageInfo.getStorageClass()+"@"+storageInfo.getHsm() ;
         String replicationSource = storageInfo.getKey("replication.source");

         ProtocolInfo originalProtocolInfo = replicate.getProtocolInfo() ;

         int matchCount = 0 ;

         synchronized( _mapLock ){

             for( Iterator i = _map.values().iterator() ; i.hasNext() ; ){

                 Entry entry = (Entry)i.next() ;

                 if( ! entry._pattern.matcher( storageClass ).matches() )continue ;

                 if( ! ( ( entry._source.equals("*")                     ) ||
                         ( entry._source.indexOf(replicationSource) > -1 )    ) )continue ;

                 matchCount ++ ;

                 _log.info("Entry found for : SC=<"+storageClass+"> source="+replicationSource+" : "+entry ) ;
                 entry._hit ++ ;

                 CellPath path = entry._path == null ? _defaultDestinationPath : entry._path ;

                 replicate.setDestinationFileStatus( entry._status ) ;

                 ProtocolInfo info = entry._info == null ? originalProtocolInfo : entry._info ;
                 replicate.setProtocolInfo( info ) ;

                 try{
                    sendMessage( new CellMessage( (CellPath)path.clone() , replicate ) )  ;
                 }catch(Exception ee ){
                    _log.warn("Problem : couldn't forward message to : "+entry._path+" : "+ee ) ;
                 }

                 if( ! entry._continue )break ;
             }
         }
         _log.info("Total match count for <"+storageClass+"> was "+matchCount ) ;

      }

   }
   public String hh_save = "" ;
   public String ac_save( Args args ) throws Exception {
      dumpSetup() ;
      return "Data written to "+_configFile ;
   }
   public String hh_ls_hop = "[<ruleName>] -count" ;
   public String ac_ls_hop_$_0_1( Args args ){

      StringBuffer sb = new StringBuffer() ;
      boolean   count = args.hasOption("count") || args.hasOption("c") ;

      synchronized( _mapLock ){

         if( args.argc() == 0 ){
            if( count ){
               for( Iterator i = _map.values().iterator() ; i.hasNext() ; ){
                   Entry e = (Entry)i.next() ;
                   sb.append( Formats.field( e._name       , 15 ,  Formats.RIGHT ) ).
                      append("   ").
                      append( Formats.field( e._patternStr , 30 ,  Formats.CENTER ) ).
                      append("   ").
                      append( Formats.field( ""+e._hit     ,  7 ,  Formats.RIGHT ) ).
                      append("\n");
               }
            }else{
               for( Iterator i = _map.values().iterator() ; i.hasNext() ; ){
                   Entry e = (Entry)i.next() ;
                   sb.append( e.toCommandString() ).append("\n");
               }
            }
         }else{
            String ruleName = args.argv(0) ;

            Entry e  = (Entry) _map.get( ruleName ) ;
            if( e == null )
              throw new
              IllegalArgumentException("Rule not found : "+ruleName);

            sb.append( e.toCommandString() ).append("\n");
         }
      }
      return sb.toString();
   }
}