/*
 * Main.java
 *
 * Created on July 19, 2004, 4:45 PM
 */

package org.dcache.srm.unixfs;

import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMAuthorization;
import diskCacheV111.srm.FileMetaData;
import diskCacheV111.srm.RequestStatus;
import org.dcache.srm.GetFileInfoCallbacks;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.ReleaseSpaceCallbacks;
import org.dcache.srm.ReserveSpaceCallbacks;
import org.dcache.srm.PrepareToPutInSpaceCallbacks;
import org.dcache.srm.SRMUser;
import diskCacheV111.srm.StorageElementInfo;
import org.dcache.srm.AdvisoryDeleteCallbacks;
import org.dcache.srm.PinCallbacks;
import org.dcache.srm.UnpinCallbacks;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.SRM;
import java.io.File;

import java.io.*;
import dmg.util.*;

/**
 *
 * @author  timur
 */
public class Main extends CommandInterpreter implements  Runnable {
    private Configuration config;
    private SRMAuthorization authorization;
    private SRM srm;
    private String name;
    /** Creates a new instance of Main */
    public Main(String[] args) throws Exception {
        String config_file = args[0];
        name = args[1];
        File f = new File(config_file);
        if(!f.exists())
        {
            Configuration configuration = new Configuration();
            configuration.write(config_file);
            System.out.println("configuration written to a file: "+config_file);
            return;
        }
        String gridftphost = args[2];
        int gridftpport = Integer.parseInt(args[3]);
	String stat=args[4];
	String chown=args[5];
        System.out.println("reading configuration from "+config_file);
        config = new Configuration(config_file);
        PrintStream out,err;
        
        if ( args.length >6) {
            String logfile = args[6];
            System.out.println("Logging to "+logfile);
            out = new PrintStream(new FileOutputStream(logfile));
            err = out;
        }
        else
        {
            System.out.println("Logging to stdout and stderr");
            out = System.out;
            err = System.err;
        }
        authorization = UnixfsAuthorization.getAuthorization(config.getKpwdfile());
        config.setAuthorization(authorization);
        Storage storage = 
            new Storage(gridftphost,gridftpport,config,stat,chown,out,err);
        config.setStorage(storage);
        
        srm = SRM.getSRM(config,name);
         new Thread(this).start();    
        
        
            
    }
    
    
    public void run() 
    { int failures = 0;
        while(failures < 100)
        {
            
        
        try
        {
        
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String nextCommand =null;
        System.out.println("<<<<Welcome to srm server admin shell>>>");
        System.out.println("type help to begin");
        System.out.print("[srm server "+name+ " ]# ");
        while ( (nextCommand = br.readLine()) != null) {
            System.out.println("Interpeting command : "+nextCommand);
            try
            {
                System.out.println(command(nextCommand));
            }catch(Throwable t)
            {
                t.printStackTrace();
            }
            System.out.print("[srm server "+name+ " ]# ");
        }
        return;
        }
        catch(Exception e)
        {
            e.printStackTrace();
            failures++;
        }
        
        }
        System.err.println("too many falures, exiting command interpreter loop");
    }
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        if(args == null || args.length <6 ||
        args[0].equalsIgnoreCase("-h")  ||
        args[0].equalsIgnoreCase("-help")  ||
        args[0].equalsIgnoreCase("--h")  ||
        args[0].equalsIgnoreCase("--help")  
        )
        {
            System.err.println("Usage: java [-classpath <CLASSPATH to all srm jars>] org.dcache.srm.unixfs  <configuration file> <instance_name> <gridftp server host> <gridftp server port> <stat command path> <chown path> [logfile] \n" +
                                "      if configuration file does not exist it will be created and program will exit \n" +
                                "       you can then review the configuration file and restart the program ");
            return;
        }

        new Main(args);
    }

           
        public void getInfo( java.io.PrintWriter printWriter ) {
            StringBuffer sb = new StringBuffer();
            sb.append("SRM Cell");
            sb.append(" storage info ");
            sb.append('\n');
            sb.append(config.toString()).append('\n');
            try {
                srm.printGetSchedulerInfo(sb);
                srm.printPutSchedulerInfo(sb);
                srm.printCopySchedulerInfo(sb);
            } catch (java.sql.SQLException sqle) {
                sqle.printStackTrace(printWriter);
            }
            printWriter.println( sb.toString()) ;
        }
        
       public String getInfo(){
         StringWriter stringWriter = new StringWriter() ;
         PrintWriter   printWriter = new PrintWriter( stringWriter ) ; 

         getInfo( printWriter ) ;
         printWriter.flush() ;
         return stringWriter.getBuffer().toString()  ;
       }
       
        public String fh_cancel= " Syntax: cancel <id> ";
        public String hh_cancel= " <id> ";
        public String ac_cancel_$_1(Args args) {
            try {
                Long id = new Long(args.argv(0));
                StringBuffer sb = new StringBuffer();
                srm.cancelRequest(sb, id);
                return sb.toString();
            }catch (Exception e) {
                //esay(e);
                return e.toString();
            }
        }
        
        public String fh_ls= " Syntax: ls [-get] [-put] [-copy] [-l] [<id>] "+
        "#will list all requests";
        public String hh_ls= " [-get] [-put] [-copy] [-l] [<id>]";
        public String ac_ls_$_0_1(Args args) {
            try {
                boolean get=args.getOpt("get") != null;
                boolean put=args.getOpt("put") != null;
                boolean copy=args.getOpt("copy") != null;
                boolean longformat = args.getOpt("l") != null;
                StringBuffer sb = new StringBuffer();
                if(args.argc() == 1) {
                    try {
                        Long reqId = new Long(args.argv(0));
                        srm.listRequest(sb, reqId, longformat);
                    }
                    catch( NumberFormatException nfe) {
                        return "id must be a nonnegative integer, you gave id="+args.argv(0);
                    }
                }
                else {
                    if( !get && !put && !copy ) {
                        get=true;
                        put=true;
                        copy=true;
                        
                    }
                    if(get) {
                        sb.append("Get Requests:\n");
                        srm.listGetRequests(sb);
                        sb.append('\n');
                    }
                    if(put) {
                        sb.append("Put Requests:\n");
                        srm.listPutRequests(sb);
                        sb.append('\n');
                    }
                    if(copy) {
                        sb.append("Copy Requests:\n");
                        srm.listCopyRequests(sb);
                        sb.append('\n');
                    }
                }
                return sb.toString();
            }
            catch(Throwable t) {
                t.printStackTrace();
                return t.toString();
            }
        }
        public String fh_ls_queues= " Syntax: ls queues [-get] [-put] [-copy] [-l]  "+
        "#will list schedule queues";
        public String hh_ls_queues= " [-get] [-put] [-copy] [-l] ";
        public String ac_ls_queues_$_0(Args args) {
            try {
                boolean get=args.getOpt("get") != null;
                boolean put=args.getOpt("put") != null;
                boolean copy=args.getOpt("copy") != null;
                boolean longformat = args.getOpt("l") != null;
                StringBuffer sb = new StringBuffer();
                 
                if( !get && !put && !copy ) {
                    get=true;
                    put=true;
                    copy=true;

                }
                if(get) {
                    sb.append("Get Request Scheduler:\n");
                    srm.printGetSchedulerThreadQueue(sb);
                    srm.printGetSchedulerPriorityThreadQueue(sb);
                    srm.printCopySchedulerReadyThreadQueue(sb);
                    sb.append('\n');
                }
                if(put) {
                    sb.append("Put Request Scheduler:\n");
                    srm.printPutSchedulerThreadQueue(sb);
                    srm.printPutSchedulerPriorityThreadQueue(sb);
                    srm.printPutSchedulerReadyThreadQueue(sb);
                    sb.append('\n');
                }
                if(copy) {
                    sb.append("Copy Request Scheduler:\n");
                    srm.printCopySchedulerThreadQueue(sb);
                    srm.printCopySchedulerPriorityThreadQueue(sb);
                    srm.printCopySchedulerReadyThreadQueue(sb);
                    sb.append('\n');
                }
                return sb.toString();
            }
            catch(Throwable t) {
                t.printStackTrace();
                return t.toString();
            }
        }
        
        public String fh_ls_completed= " Syntax: ls completed [-get] [-put] [-copy] [-l] [max_count]"+
        " #will list completed (done, failed or canceled) requests, if max_count is not specified, it is set to 50";
        public String hh_ls_completed= " [-get] [-put] [-copy] [-l] [max_count]";
        public String ac_ls_completed_$_0_1(Args args) throws Exception{
            boolean get=args.getOpt("get") != null;
            boolean put=args.getOpt("put") != null;
            boolean copy=args.getOpt("copy") != null;
            boolean longformat = args.getOpt("l") != null;
            int max_count=50;
            if(args.argc() == 1) {
                max_count = Integer.parseInt(args.argv(0));
            }
            
            if( !get && !put && !copy ) {
                get=true;
                put=true;
                copy=true;
                
            }
            StringBuffer sb = new StringBuffer();
            if(get) {
                sb.append("Get Requests:\n");
                srm.listLatestCompletedGetRequests(sb, max_count);
                sb.append('\n');
            }
            if(put) {
                sb.append("Put Requests:\n");
                srm.listLatestCompletedPutRequests(sb, max_count);
                sb.append('\n');
            }
            if(copy) {
                sb.append("Copy Requests:\n");
                srm.listLatestCompletedCopyRequests(sb, max_count);
                sb.append('\n');
            }
            return sb.toString();
        }
     public String hh_info = "[-l|-a]" ;
   public String ac_info( Args args ) throws Exception {
       return getInfo();
   }
        public String fh_exit= " Syntax: exit "+
        " #will stop the server and exit the shell";
        public String hh_exit= " ";
        public String ac_exit_$_0_1(Args args) throws Exception{
            System.exit(0);
            return "exiting";
        }
 
}