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

import dmg.util.Args;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.cells.CellCommandListener;
import org.dcache.cells.CellStub;
import org.dcache.util.Glob;						//added
import org.dcache.util.list.DirectoryEntry;				//added
import org.dcache.util.list.DirectoryStream;				//added
import org.dcache.util.list.ListDirectoryHandler;			//added
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.springframework.beans.factory.annotation.Required;

import java.util.Collections;

public class CDMI
    implements CellCommandListener
{
    private boolean isDefaultFormal;

    private CellStub poolManager;				//added
    private PnfsHandler pnfs;

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

   // PoolManager Start, property should be poolManager. Has a problem.
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
    // PoolManager End

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
