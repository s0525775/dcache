package org.dcache.cdmi;

import java.util.ArrayList;
import java.util.Collection;						//added
import java.util.concurrent.Callable;
import com.google.common.collect.Range;					//added

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;					//added
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupMessage;    //added
import diskCacheV111.vehicles.PoolManagerPoolInformation;               //added
import dmg.cells.nucleus.CellPath;                                      //added
import dmg.cells.nucleus.DelayedReply;                                  //added
import dmg.cells.nucleus.Reply;                                         //added

import dmg.util.Args;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetBoundException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellStub;
import org.dcache.util.list.DirectoryEntry;				//added
import org.dcache.util.list.ListDirectoryHandler;			//added
import org.springframework.beans.factory.annotation.Required;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.logging.Level;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.AbstractMessageCallback;                        //added
import org.dcache.cells.CellMessageReceiver;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryListSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
//import org.dcache.vehicles.FileAttributes;

public class CDMI extends AbstractCellComponent
    implements Runnable, CellCommandListener, CellMessageReceiver
{

    private boolean isDefaultFormal;
    private CellStub poolManager;
    private CellStub helloStub;
    private CellStub pnfsStub;
    private PnfsHandler pnfs;
    private ListDirectoryHandler lister;
    private ServerSocketChannel channel;
    private CellStub billing;
    private CellStub pool;
    private DirectoryListSource list;
    private String result = "";

    private final static Logger _log =
        LoggerFactory.getLogger(CDMI.class);

    public boolean isDefaultFormal()
    {
        return isDefaultFormal;
    }

    public void setDefaultFormal(boolean defaultFormal)
    {
        isDefaultFormal = defaultFormal;
    }

    public static final String hh_hello = "[-formal] <name> # prints greeting";
    public static final String fh_hello = "Prints a greeting for NAME. Outputs a formal greeting if -formal is given.";
    public String ac_hello_$_1(Args args)
    {
        String name = args.argv(0);
        String formal = args.getOption("formal");
        boolean isFormal = (formal != null) ? formal.equals("") || Boolean.parseBoolean(formal) : isDefaultFormal;
        if (isFormal) {
            return "Hello, " + name;
        } else {
            return "Hi there, " + name;
        }
    }

    @Required
    public void setPoolManager(CellStub poolManager)
    {
        this.poolManager = poolManager;
    }

    @Required
    public void setPnfsHandler(PnfsHandler handler)
    {
        this.pnfs = handler;
    }

    @Required
    public void setPnfsStub(CellStub pnfsStub)
    {
        this.pnfsStub = pnfsStub;
    }

    @Required
    public void setHelloStub(CellStub helloStub)
    {
        this.helloStub = helloStub;
    }

    @Required
    public void setBilling(CellStub billing)
    {
        this.billing = billing;
    }

    @Required
    public void setPool(CellStub pool)
    {
        this.pool = pool;
    }

    @Required
    public void setListDirectoryHandler(ListDirectoryHandler lister)
    {
        this.lister = lister;
    }

    public CDMIMessage messageArrived(CDMIMessage message)
    {
        String name = message.getName();
        message.setGreeting(isDefaultFormal ? "Hello, " + name : "Hi there, " + name);
        return message;
    }

    /*
     * Would have conflict with next function
    @Command(name = "hi", hint = "prints greeting", usage = "Prints a greeting for NAME.")
    class HelloCommand implements Callable<String>
    {
        @Argument(help = "Your name")
        String name;

        @Option(name="formal", usage = "Output a formal greeting")
        boolean isFormal = isDefaultFormal;

        @Override
        public String call()
        {
            if (isFormal) {
                return "Hello, " + name;
            } else {
                return "Hi there, " + name;
            }
        }
    }
    */

    /* 7
    @Command(name = "hi", hint = "prints greeting", usage = "Prints a greeting for NAME.")
    class HelloCommand implements Callable<String>
    {
        @Argument(index = 0, help = "Your name")
        String name;

        @Argument(index = 1, help = "Name of a cdmi cell")
        String cell;

        @Override
        public String call() throws CacheException, InterruptedException
        {
            return helloStub.sendAndWait(new CellPath(cell), new CDMIMessage(name)).getGreeting();
        }
    } */

    /* 8
    @Command(name = "hi", hint = "prints greeting", usage = "Prints a greeting for NAME.")
    class HelloCommand extends DelayedReply implements Callable<Reply>, Runnable
    {
        @Argument(index = 0, help = "Your name")
        String name;

        @Argument(index = 1, help = "Name of a hello cell")
        String cell;

        @Override
        public Reply call()
        {
            new Thread(this).start();
            return this;
        }

        @Override
        public void run()
        {
             try {
                 reply(helloStub.sendAndWait(new CellPath(cell), new CDMIMessage(name)).getGreeting());
             } catch (CacheException e) {
                 reply(e);
             } catch (InterruptedException ex) {
                 Logger.getLogger(CDMI.class.getName()).log(Level.SEVERE, null, ex);
             }
        }
    } */

    /* 9
    @Command(name = "hi", hint = "prints greeting", usage = "Prints a greeting for NAME.")
    class HelloCommand extends DelayedReply implements Callable<Reply>
    {
        @Argument(index = 0, help = "Your name")
        String name;

        @Argument(index = 1, help = "Name of a cdmi cell")
        String cell;

        @Override
        public Reply call() throws CacheException, InterruptedException
        {
            helloStub.send(new CellPath(cell), new CDMIMessage(name),
                    CDMIMessage.class, new AbstractMessageCallback<CDMIMessage>()
            {
                @Override
                public void success(CDMIMessage message)
                {
                    reply(message.getGreeting());
                }

                @Override
                public void failure(int rc, Object error)
                {
                    reply(error.toString() + "(" + rc + ")");
                }
            });
            return this;
        }
    } */

    /* 10
    @Command(name = "hi", hint = "prints greeting", usage = "Prints a greeting for NAME.")
    class HelloCommand extends DelayedCommand<String>
    {
        @Argument(index = 0, help = "Your name")
        String name;

        @Argument(index = 1, help = "Name of a cdmi cell")
        String cell;

        @Override
        public String execute() throws CacheException, InterruptedException
        {
            return helloStub.sendAndWait(new CellPath(cell), new CDMIMessage(name)).getGreeting();
        }
    } */

    @Command(name = "hi", hint = "prints greeting",
             usage = "Prints a greeting for NAME.")
    class HelloCommand extends DelayedReply implements Callable<Reply>
    {
        @Argument(index = 0, help = "Your name")
        String name;

        @Argument(index = 1, help = "Name of a hello cell")
        String cell;

        @Override
        public Reply call() throws CacheException, InterruptedException
        {
            helloStub.send(new CellPath(cell), new CDMIMessage(name),
                    CDMIMessage.class, new AbstractMessageCallback<CDMIMessage>()
            {
                @Override
                public void success(CDMIMessage message)
                {
                    reply(message.getGreeting());
                }

                @Override
                public void failure(int rc, Object error)
                {
                    reply(error.toString() + "(" + rc + ")");
                }
            });
            return this;
        }
    }

    // works! - Start
    public void start()
    {
        new Thread(this).start();
    }

    public void stop()
    {
    }

    @Override
    public void run()
    {
        try {
            Test.write("/tmp/outputtest.log", "New:");
            PrintWriter out2 = new PrintWriter(new OutputStreamWriter(new FileOutputStream("/tmp/outputtest.log"), "UTF-8"));
            out2.flush();
            lister.printDirectory(Subjects.ROOT, new ListPrinter(out2), new FsPath("/"), null, Range.<Integer>all());
            out2.close();
        } catch (FileNotFoundException | InterruptedException | CacheException | UnsupportedEncodingException ex) {
            Test.write("/tmp/outputtest.log", ex.getMessage());
        }
    }
    // works! - End

    private static class ListPrinter implements DirectoryListPrinter
    {
        private final PrintWriter writer;

        private ListPrinter(PrintWriter writer)
        {
            Test.write("/tmp/test005.log", "T008");
            this.writer = writer;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            Test.write("/tmp/test005.log", "T009");
            return EnumSet.noneOf(FileAttribute.class);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
                throws InterruptedException
        {
            Test.write("/tmp/test005.log", "T010");
            writer.println(entry.getName());
            Test.write("/tmp/test005.log", "T011");
            Test.write("/tmp/test006.log", "Writer:" + entry.getName());
        }
    }

    /*
    public void start() throws IOException
    {
        //passed
        channel = ServerSocketChannel.open();
        channel.bind(new InetSocketAddress(2000));
        //next line is added from me, NEW, switches to non-blocking mode
        channel.configureBlocking(false);
        new Thread(this).start();
    }

    public void stop() throws IOException
    {
        //passed, works
        if (channel != null) {
            channel.close();
        }
    }

    @Override
    public void run()
    {
        //Test
        try {
            Test.write("/tmp/outputtest.log", "New:");
            PrintWriter out2 = new PrintWriter(new OutputStreamWriter(new FileOutputStream("/tmp/outputtest.log"), "UTF-8"));
            out2.flush();
            lister.printDirectory(Subjects.ROOT, new ListPrinter(out2), new FsPath("/"), null, Range.<Integer>all());
            out2.close();
        } catch (FileNotFoundException | InterruptedException | CacheException | UnsupportedEncodingException ex) {
            Test.write("/tmp/outputtest.log", ex.getMessage());
        }
        //Test
        while (true) {
            //passed, works
            try {
                //it will work till this comment line
                //the next line doesn't throw an exception or a response in blocking mode! it will wait till there is a connection to accept. but there doesn't seem to be a connection to accept
                SocketChannel connection = channel.accept();
                if (connection != null) { //connection is null, this line of code is rewritten for non-blocking mode, NEW
                    PrintWriter out = new PrintWriter(new OutputStreamWriter(connection.socket().getOutputStream()));
                    out.flush();
                    try {
                        //old
                        lister.printDirectory(Subjects.ROOT, new ListPrinter(out), new FsPath("/"), null, Range.<Integer>all());

                        //new
                        /*
                        BufferedReader reader = new BufferedReader(new InputStreamReader(connection.socket().getInputStream()));
                        int port = Integer.parseInt(reader.readLine());

                        Transfer transfer = new Transfer(pnfs, Subjects.ROOT, new FsPath("/disk/" + System.currentTimeMillis()));
                        transfer.setClientAddress((InetSocketAddress) connection.getRemoteAddress());
                        transfer.setBillingStub(billing);
                        transfer.setPoolManagerStub(poolManager);
                        transfer.setPoolStub(pool);
                        transfer.setCellName(getCellName());
                        transfer.setDomainName(getCellDomainName());
                        InetSocketAddress address = new InetSocketAddress(((InetSocketAddress) connection
                                .getRemoteAddress()).getAddress(), port);
                        transfer.setProtocolInfo(new CDMIProtocolInfo(address));
                        transfer.createNameSpaceEntry();
                        transfer.selectPoolAndStartMover(null, TransferRetryPolicies.tryOncePolicy(1000));
                    } catch (IOException | CacheException e) {
                        */ /*
                    } catch (CacheException e) {
                        out.println(e.toString());
                    }
                    out.flush();
                    out.close();
                }
                //added, NEW
                if (connection != null && connection.isConnected()) {
                    connection.close();
                }
            } catch (NotYetBoundException e) {
                // log error
                break;
            } catch (InterruptedException | ClosedChannelException e) {
                // log error
                break;
            } catch (IOException e) {
                // log error
            }
        }
    }
    */

    @Command(name = "pools", hint = "show pools in pool group",
             usage = "Shows the names of the pools in POOLGROUP.")
    class PoolsCommands implements Callable<ArrayList<String>>
    {
        @Argument(help = "A pool group")
        String poolgroup;

        @Override
        public ArrayList<String> call() throws CacheException, InterruptedException
        {
            Iterable<String> poolgroups = Collections.singleton(poolgroup);
            PoolManagerGetPoolsByPoolGroupMessage request = new PoolManagerGetPoolsByPoolGroupMessage(poolgroups);
            PoolManagerGetPoolsByPoolGroupMessage reply = poolManager.sendAndWait(request);
            ArrayList<String> names = new ArrayList<>();
            for (PoolManagerPoolInformation pool : reply.getPools()) {
                names.add(pool.getName());
            }
            return names;
        }
    }

    public CellStub getPoolManager()
    {
        return poolManager;
    }

    @Command(name = "mkdir", hint = "create directory",
             usage = "Create a new directory")
    class MkdirCommand implements Callable<String>
    {
        @Argument(help = "Directory name")
        String name;

        @Override
        public String call() throws CacheException
        {
            pnfs.createPnfsDirectory(name);
            return "";
        }
    }

    @Command(name = "rm", hint = "delete directory (Test!)",
             usage = "Delete a directory")
    class RmCommand implements Callable<String>
    {
        @Argument(help = "Directory name")
        String name;

        @Override
        public String call() throws CacheException
        {
            pnfs.deletePnfsEntry(name);
            return "";
        }
    }

    /*
    @Command(name = "ls", hint = "list directory (Test!)",
             usage = "List directories")
    class LsCommand extends DelayedReply implements Callable<Reply>, Runnable
    {
        @Argument(help = "Directory name")
        String name;

        @Override
        public Reply call()
        {
            new Thread(this).start();
            return this;
        }

        @Override
        public void run()
        {
             try {
                 String cell = "cdmi";
                 reply(helloStub.sendAndWait(new CellPath(cell), new CDMIMessage(name)).getGreeting());
             } catch (CacheException e) {
                 reply(e);
             } catch (InterruptedException ex) {
             }
        }
    } */

    // runs too fast - getRequiredAttributes() - interrupts
    @Command(name = "ls2", hint = "list directory (Test!)",
             usage = "List directories")
    class LsCommand extends DelayedReply implements Callable<String>
    {
        @Argument(help = "Directory name")
        String name;

        @Override
        public String call()
        {
            Test.write("/tmp/test007.log", "T001");
            FsPath path = new FsPath(name);
            Test.write("/tmp/test007.log", "T002");
            ListThread thread = new ListThread(path);
            Test.write("/tmp/test007.log", "T003");
            Thread newThread = new Thread(thread);
            Test.write("/tmp/test007.log", "T004");
            newThread.start();
            Test.write("/tmp/test007.log", "T004_1");
            try {
                Test.write("/tmp/test007.log", "T004_2");
                newThread.join();
                Test.write("/tmp/test007.log", "T004_3");
            } catch (InterruptedException ex) {
                Test.write("/tmp/test007.log", "T004_4" + ex.getMessage());
            }
            Test.write("/tmp/test007.log", "T005");
            return result;
        }
    }

    /**
     * List task that can serve as a DelayedReply.
     */
    class ListThread extends DelayedReply implements Runnable
    {
        private final FsPath path;

        public ListThread(FsPath path)
        {
            Test.write("/tmp/test007.log", "T006");
            this.path = path;
        }

        @Override
        public void run()
        {
            Test.write("/tmp/test007.log", "T007");
            try {
                Test.write("/tmp/test007.log", "T007_1");
                result = list(path);
                Test.write("/tmp/test007.log", "T007_2");
            } catch (CacheException ex) {
                Test.write("/tmp/test007.log", "T007_3" + ex.getMessage());
            }
            Test.write("/tmp/test007.log", "T008");
        }
    }

    /**
     * List a directory.
     */
    private String list(FsPath path) throws CacheException
    {
        StringBuilder sb = new StringBuilder();
        Test.write("/tmp/test007.log", "T009");
        try {
            Test.write("/tmp/test007.log", "T010");  //here
            list.printDirectory(Subjects.ROOT, new DirectoryPrinter(sb), path, null, Range.<Integer>all());
            Test.write("/tmp/test007.log", "T011");
        } catch (InterruptedException ex) {
            Test.write("/tmp/test007.log", "T012:" + ex.getMessage());
        } catch (NotDirCacheException ex) {
            Test.write("/tmp/test007.log", "T012_1:" + ex.getMessage());
        }
        Test.write("/tmp/test007.log", "T013");
        return sb.toString();
    }

    /**
     * Reply format for directory listings.
     */
    class DirectoryPrinter implements DirectoryListPrinter
    {
        private final StringBuilder out;

        public DirectoryPrinter(StringBuilder out)
        {
            Test.write("/tmp/test007.log", "T014");
            this.out = out;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            Test.write("/tmp/test007.log", "T015");
            return EnumSet.noneOf(FileAttribute.class);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) throws InterruptedException
        {
            Test.write("/tmp/test007.log", "T016");
            FileAttributes attr = entry.getFileAttributes();
            Test.write("/tmp/test007.log", "T017");
            out.append(entry.getName()).append("\n");
            Test.write("/tmp/test008.log", "Writer:" + entry.getName());
        }
    }

    /**
     * List task that can serve as a DelayedReply.
     */
    class ListThread2 extends DelayedReply implements Runnable
    {
        private final FsPath path;

        public ListThread2(FsPath path)
        {
            Test.write("/tmp/test007.log", "T105");
            this.path = path;
        }

        @Override
        public void run()
        {
            try {
                Test.write("/tmp/test007.log", "T106");
                reply(list(path));
            } catch (CacheException ex) {
                Test.write("/tmp/test007.log", "T107");
                reply(ex);
            }
        }
    }

    // commands
    // diskCacheV111.admin.UserAdminShell - dmg.util.RequestTimeOutException
    public final static String hh_ls_$_1 = "ls <path>";
    public DelayedReply ac_ls_$_1(Args args)
    {
        Test.write("/tmp/test007.log", "T101");
        FsPath path = new FsPath(args.argv(0));
        Test.write("/tmp/test007.log", "T102");
        ListThread2 thread = new ListThread2(path);
        Test.write("/tmp/test007.log", "T103");
        new Thread(thread).start();
        Test.write("/tmp/test007.log", "T104");
        return thread;
    }

}
