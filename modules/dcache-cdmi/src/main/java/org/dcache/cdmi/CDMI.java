package org.dcache.cdmi;

import java.util.ArrayList;
import java.util.Collection;						//added
import java.util.concurrent.Callable;
import com.google.common.collect.Range;					//added

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FsPath;					//added
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.PoolManagerGetPoolsByPoolGroupMessage;    //added
import diskCacheV111.vehicles.PoolManagerPoolInformation;               //added
import dmg.cells.nucleus.CellPath;                                      //added
import dmg.cells.nucleus.DelayedReply;                                  //added
import dmg.cells.nucleus.Reply;                                         //added

import dmg.util.Args;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.DelayedCommand;                                 //added
import dmg.util.command.Option;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NotYetBoundException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellStub;
import org.dcache.util.Glob;						//added
import org.dcache.util.list.DirectoryEntry;				//added
import org.dcache.util.list.DirectoryStream;				//added
import org.dcache.util.list.ListDirectoryHandler;			//added
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.springframework.beans.factory.annotation.Required;
import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.dcache.auth.Subjects;
import org.dcache.cells.AbstractMessageCallback;                        //added
import org.dcache.util.list.DirectoryListPrinter;
//import org.dcache.vehicles.FileAttributes;

public class CDMI
    implements CellCommandListener, Runnable
{
    private boolean isDefaultFormal;

    private CellStub poolManager;				//added
    private CellStub helloStub;                                 //added
    private PnfsHandler pnfs;
    ListDirectoryHandler lister;                                //added
    ServerSocketChannel channel;                                //added

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

    @Required //Required annotation
    public void setHelloStub(CellStub helloStub)
    {
        this.helloStub = helloStub;
    }

    public CDMIMessage messageArrived(CDMIMessage message)
    {
        String name = message.getName();
        message.setGreeting(isDefaultFormal ? "Hello, " + name : "Hi there, " + name);
        return message;
    }

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

    @Command(name = "hi", hint = "prints greeting", usage = "Prints a greeting for NAME.")
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

    @Required
    public void setListDirectoryHandler(ListDirectoryHandler lister)
    {
        this.lister = lister;
    }

    public void start() throws IOException
    {
        channel = ServerSocketChannel.open();
        channel.bind(new InetSocketAddress(2000));
        new Thread(this).start();
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
        while (true) {
            try {
                try (SocketChannel connection = channel.accept()) {
                    PrintWriter out = new PrintWriter(new OutputStreamWriter(connection.socket()
                            .getOutputStream()));
                    try {
                        lister.printDirectory(Subjects.ROOT, new ListPrinter(out), new FsPath("/"),
                                null, Range.<Integer>all());
                    } catch (CacheException e) {
                        out.println(e.toString());
                    }
                    out.flush();
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
    }

    private class ListPrinter implements DirectoryListPrinter  //cannot be abstract
    {
        private final PrintWriter writer;

        private ListPrinter(PrintWriter writer)
        {
            this.writer = writer;
        }

        @Override
        public Set<org.dcache.namespace.FileAttribute> getRequiredAttributes()  //in conflict with java.nio.file.attribute.FileAttribute
        {
            return EnumSet.noneOf(FileAttribute.class);
        }

        @Override
        public void print(FsPath dir, FileAttributes dirAttr, DirectoryEntry entry)
                throws InterruptedException
        {
            writer.println(entry.getName());
        }
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

    @Required
    public void setPoolManager(CellStub poolManager)
    {
        this.poolManager = poolManager;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfs)
    {
        this.pnfs = pnfs;
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

    // DCache sends a message and CDMI doesn't answer it, still testing
    /*
    // old code
    @Command(name = "dir", hint = "list directories (Test!)",
            usage = "List directories")
    class DirCommand implements Callable<ArrayList<String>>
    {
        @Argument(help = "Directory name")
        String name;

        @Override
        public ArrayList<String> call() throws CacheException {
             //the other three parameters seem to be filters for search like pattern, range and attributes - necessary for test?
             PnfsListDirectoryMessage request = new PnfsListDirectoryMessage(name, null, null, null);
             Collection<DirectoryEntry> entries = request.getEntries();
             ArrayList<String> names = new ArrayList<>();
             for (DirectoryEntry entry : entries) {
                  names.add(entry.getName());
             }
             return names;
        }
    }
    */
    // new code
    @Command(name = "dir", hint = "list directories (Test!)",
             usage = "List directories")
    class DirCommand implements Callable<ArrayList<String>>
    {
	// Argument necessary? --> Look out for example 13 of Gerd's tutorial, maybe this solves the message problem since he implements InetSockets.
	// ListDirectoryHandler and PnfsListDirectoryMessage need to work together somehow.
        @Argument(help = "Directory name")
        String name;

        @Override
        public ArrayList<String> call() throws CacheException, InterruptedException {
       	     FsPath fspath = new FsPath();
             fspath.add(name);
             Glob glob = new Glob("*");
             ListDirectoryHandler handler = new ListDirectoryHandler(pnfs);
             DirectoryStream dStream = handler.list(null, fspath, glob, Range.<Integer>all());
             ArrayList<String> names = new ArrayList<>();
             for (DirectoryEntry entry : dStream) {
                  names.add(entry.getName());
             }
             return names;
        }
    }
}
