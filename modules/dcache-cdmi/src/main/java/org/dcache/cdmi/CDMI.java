package org.dcache.cdmi;

import org.dcache.cdmi.temp.Test;
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
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
import org.dcache.util.Transfer;
import org.dcache.util.TransferRetryPolicies;
import org.dcache.util.list.DirectoryListPrinter;
import org.dcache.util.list.DirectoryListSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.namespace.FileType.*;


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

    private static class ListPrinter implements DirectoryListPrinter
    {
        private final PrintWriter writer;

        private ListPrinter(PrintWriter writer)
        {
            this.writer = writer;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.of(TYPE, SIZE);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
                throws InterruptedException
        {
            writer.println(entry.getName());
            FileAttributes attr = entry.getFileAttributes();
            if (attr.getFileType() == DIR) {
                writer.println(entry.getName() + "::d::" + attr.getSize() + "\n");
                Test.write("/tmp/test006.log", "Writer:" + entry.getName() + "::d::" + attr.getSize() + "\n");
            } if (attr.getFileType() == REGULAR) {
                writer.println(entry.getName() + "::f::" + attr.getSize() + "\n");
                Test.write("/tmp/test006.log", "Writer:" + entry.getName() + "::f::" + attr.getSize() + "\n");
            } else {
                writer.println(entry.getName() + "::s::" + attr.getSize() + "\n");
            }
        }
    }

    public void start() throws IOException
    {
        channel = ServerSocketChannel.open();
        channel.bind(new InetSocketAddress(2000));
        //new Thread(this).start();
    }

    public void stop() throws IOException
    {
        if (channel != null) {
            channel.close();
        }
    }

    @Override
    public void run()
    {
        /*
        while (true) {
            try {
                try (SocketChannel connection = channel.accept()) {
                    //PrintWriter out = new PrintWriter(new OutputStreamWriter(connection.socket().getOutputStream()));
                    try {
                        //lister.printDirectory(Subjects.ROOT, new ListPrinter(out), new FsPath("/"),
                        //                      null, Range.<Integer>all());

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
                        //out.println(e.toString());
                    }
                    //out.flush();
                }
            } catch (NotYetBoundException e) {
                // log error
                break;
            } catch (InterruptedException | ClosedChannelException e) {
                break;
            } catch (IOException e) {
                // log error
            }
        }
        */
    }

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

    // runs too fast - getRequiredAttributes() - interrupts
    @Command(name = "ls", hint = "list directory (Test!)",
             usage = "List directories")
    class LsCommand extends DelayedReply implements Callable<String>
    {
        @Argument(help = "Directory name")
        String name;

        @Override
        public String call()
        {
            FsPath path = new FsPath(name);
            new Thread(new ListThread(path)).start();
            return result;
        }
    }

    /**
     * List task that can serve as a DelayedReply.
     */
    class ListThread implements Runnable
    {
        private final FsPath path;

        public ListThread(FsPath path)
        {
            this.path = path;
        }

        @Override
        public void run()
        {
            try {
                result = list(path);
            } catch (CacheException ex) {
            }
        }
    }

    /**
     * List a directory.
     */
    private String list(FsPath path) throws CacheException
    {
        StringBuilder sb = new StringBuilder();
        try {
            list.printDirectory(Subjects.ROOT, new DirectoryPrinter(sb), path, null, Range.<Integer>all());
        } catch (InterruptedException | NotDirCacheException ex) {
        }
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
            this.out = out;
        }

        @Override
        public Set<FileAttribute> getRequiredAttributes()
        {
            return EnumSet.of(TYPE, SIZE);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry) throws InterruptedException
        {
            FileAttributes attr = entry.getFileAttributes();
            if (attr.getFileType() == DIR) {
                out.append(entry.getName()).append("::d::").append(attr.getSize()).append("\n");
            } else if (attr.getFileType() == REGULAR) {
                out.append(entry.getName()).append("::f::").append(attr.getSize()).append("\n");
            } else {
                out.append(entry.getName()).append("::s::").append(attr.getSize()).append("\n");
            }
            Test.write("/tmp/test006.log", "Writer2:" + entry.getName());
        }
    }

}
