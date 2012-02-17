package org.dcache.tests.poolmanager;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.poolManager.PoolSelectionUnitV2;
import diskCacheV111.poolManager.PoolSelectionUnit.SelectionPool;
import diskCacheV111.vehicles.GenericStorageInfo;
import diskCacheV111.vehicles.StorageInfo;

import dmg.util.Args;
import dmg.util.CommandException;
import dmg.util.CommandExitException;
import dmg.util.CommandInterpreter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class PoolSelectionUnitTest {

    private final PoolSelectionUnitV2 _psu = new PoolSelectionUnitV2();
    private final CommandInterpreter _ci = new CommandInterpreter(_psu);



    @Before
    public void setUp() throws Exception {

        Logger.getLogger("logger.org.dcache.poolselection").setLevel(Level.DEBUG);
        // storage units

        _ci.command( new Args("psu create unit -store  h1:u1@osm" )  );
        _ci.command( new Args("psu create unit -store  h1:u2@osm" )  );

        _ci.command( new Args("psu create unit -store  zeus:u1@osm" )  );
        _ci.command( new Args("psu create unit -store  zeus:u2@osm" )  );

        _ci.command( new Args("psu create unit -store  flc:u1@osm" )  );
        _ci.command( new Args("psu create unit -store  flc:u2@osm" )  );

        _ci.command( new Args("psu create unit -store  hermes:u1@osm" )  );
        _ci.command( new Args("psu create unit -store  hermes:u2@osm" )  );

        _ci.command( new Args("psu create unit -store  herab:u1@osm" )  );
        _ci.command( new Args("psu create unit -store  herab:u2@osm" )  );

        _ci.command( new Args("psu create unit -store  *@*" )  );

        // store unit groups

        _ci.command( new Args("psu create ugroup all-h1" )  );
        _ci.command( new Args("psu create ugroup all-zeus" )  );
        _ci.command( new Args("psu create ugroup all-flc" )  );
        _ci.command( new Args("psu create ugroup all-hermes" )  );
        _ci.command( new Args("psu create ugroup all-herab" )  );
        _ci.command( new Args("psu create ugroup all-hera" )  );
        _ci.command( new Args("psu create ugroup all" )  );

        // populate ugroups

        _ci.command( new Args("psu addto ugroup all-h1 h1:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-h1 h1:u2@osm" )  );

        _ci.command( new Args("psu addto ugroup all-h1 zeus:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-h1 zeus:u2@osm" )  );

        _ci.command( new Args("psu addto ugroup all-h1 flc:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-h1 flc:u2@osm" )  );

        _ci.command( new Args("psu addto ugroup all-h1 hermes:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-h1 hermes:u2@osm" )  );

        _ci.command( new Args("psu addto ugroup all-h1 herab:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-h1 herab:u2@osm" )  );

        _ci.command( new Args("psu addto ugroup all h1:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all h1:u2@osm" )  );
        _ci.command( new Args("psu addto ugroup all zeus:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all zeus:u2@osm" )  );
        _ci.command( new Args("psu addto ugroup all flc:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all flc:u2@osm" )  );
        _ci.command( new Args("psu addto ugroup all hermes:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all hermes:u2@osm" )  );
        _ci.command( new Args("psu addto ugroup all herab:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all herab:u2@osm" )  );
        _ci.command( new Args("psu addto ugroup all *@*" )  );

        _ci.command( new Args("psu addto ugroup all-hera h1:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-hera h1:u2@osm" )  );
        _ci.command( new Args("psu addto ugroup all-hera zeus:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-hera zeus:u2@osm" )  );
        _ci.command( new Args("psu addto ugroup all-hera hermes:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-hera hermes:u2@osm" )  );
        _ci.command( new Args("psu addto ugroup all-hera herab:u1@osm" )  );
        _ci.command( new Args("psu addto ugroup all-hera herab:u2@osm" )  );



        // network
        _ci.command( new Args("psu create unit -net    131.169.0.0/255.255.0.0" )  );
        _ci.command( new Args("psu create unit -net    0.0.0.0/0.0.0.0" )  );

        // net groups
        _ci.command( new Args("psu create ugroup intern" )  );
        _ci.command( new Args("psu create ugroup extern" )  );

        // populate net groups
        _ci.command( new Args("psu addto ugroup intern 131.169.0.0/255.255.0.0" )  );
        _ci.command( new Args("psu addto ugroup extern 0.0.0.0/0.0.0.0" )  );

        // pools
        _ci.command( new Args("psu create pool h1-read" )  );
        _ci.command( new Args("psu create pool h1-write" )  );

        _ci.command( new Args("psu create pool zeus-read" )  );
        _ci.command( new Args("psu create pool zeus-write" )  );

        _ci.command( new Args("psu create pool flc-read" )  );
        _ci.command( new Args("psu create pool flc-write" )  );

        _ci.command( new Args("psu create pool hermes-read" )  );
        _ci.command( new Args("psu create pool hermes-write" )  );

        _ci.command( new Args("psu create pool herab-read" )  );
        _ci.command( new Args("psu create pool herab-write" )  );

        _ci.command( new Args("psu create pool default-read" )  );
        _ci.command( new Args("psu create pool default-write" )  );


        // pool groups

        _ci.command( new Args("psu create pgroup h1-read-pools" )  );
        _ci.command( new Args("psu create pgroup h1-write-pools" )  );

        _ci.command( new Args("psu create pgroup zeus-read-pools" )  );
        _ci.command( new Args("psu create pgroup zeus-write-pools" )  );

        _ci.command( new Args("psu create pgroup flc-read-pools" )  );
        _ci.command( new Args("psu create pgroup flc-write-pools" )  );

        _ci.command( new Args("psu create pgroup hermes-read-pools" )  );
        _ci.command( new Args("psu create pgroup hermes-write-pools" )  );

        _ci.command( new Args("psu create pgroup herab-read-pools" )  );
        _ci.command( new Args("psu create pgroup herab-write-pools" )  );

        _ci.command( new Args("psu create pgroup default-read-pools" )  );
        _ci.command( new Args("psu create pgroup default-write-pools" )  );

        // Populate pool groups

        _ci.command( new Args("psu addto pgroup h1-read-pools h1-read" )  );
        _ci.command( new Args("psu addto pgroup h1-write-pools h1-write" )  );

        _ci.command( new Args("psu addto pgroup zeus-read-pools zeus-read" )  );
        _ci.command( new Args("psu addto pgroup zeus-write-pools zeus-write" )  );

        _ci.command( new Args("psu addto pgroup flc-read-pools flc-read" )  );
        _ci.command( new Args("psu addto pgroup flc-write-pools flc-write" )  );

        _ci.command( new Args("psu addto pgroup hermes-read-pools hermes-read" )  );
        _ci.command( new Args("psu addto pgroup hermes-write-pools hermes-write" )  );

        _ci.command( new Args("psu addto pgroup herab-read-pools herab-read" )  );
        _ci.command( new Args("psu addto pgroup herab-write-pools herab-write" )  );

        _ci.command( new Args("psu addto pgroup default-read-pools default-read" )  );
        _ci.command( new Args("psu addto pgroup default-write-pools default-write" )  );


        // links

        _ci.command( new Args("psu create link h1-read-link all-h1 intern" )  );
        _ci.command( new Args("psu create link h1-write-link all-h1 intern" )  );

        _ci.command( new Args("psu create link zeus-read-link all-zeus intern" )  );
        _ci.command( new Args("psu create link zeus-write-link all-zeus intern" )  );

        _ci.command( new Args("psu create link flc-read-link all-flc intern" )  );
        _ci.command( new Args("psu create link flc-write-link all-flc intern" )  );

        _ci.command( new Args("psu create link hermes-read-link all-hermes intern" )  );
        _ci.command( new Args("psu create link hermes-write-link all-hermes intern" )  );

        _ci.command( new Args("psu create link herab-read-link all-herab intern" )  );
        _ci.command( new Args("psu create link herab-write-link all-herab intern" )  );

        _ci.command( new Args("psu create link default-read-link-in all intern" )  );
        _ci.command( new Args("psu create link default-write-link-in all intern" )  );

        _ci.command( new Args("psu create link default-read-link-ex all extern" )  );
        _ci.command( new Args("psu create link default-write-link-ex all extern" )  );


        // link preferences
        /*
         * schema here is the classic case:
         * 		write into write-pools
         * 		read from read-pools
         * 	fallback: default-pools
         */

        _ci.command( new Args("psu set link h1-read-link         -readpref=20 -writepref=0 -cachepref=20" )  );
        _ci.command( new Args("psu set link zeus-read-link       -readpref=20 -writepref=0 -cachepref=20" )  );
        _ci.command( new Args("psu set link flc-read-link        -readpref=20 -writepref=0 -cachepref=20" )  );
        _ci.command( new Args("psu set link hermes-read-link     -readpref=20 -writepref=0 -cachepref=20" )  );
        _ci.command( new Args("psu set link herab-read-link      -readpref=20 -writepref=0 -cachepref=20" )  );
        _ci.command( new Args("psu set link default-read-link-in -readpref=1  -writepref=0 -cachepref=20" )  );
        _ci.command( new Args("psu set link default-read-link-ex -readpref=1  -writepref=0 -cachepref=20" )  );

        _ci.command( new Args("psu set link h1-write-link         -writepref=20 -readpref=0 -cachepref=0" )  );
        _ci.command( new Args("psu set link zeus-write-link       -writepref=20 -readpref=0 -cachepref=0" )  );
        _ci.command( new Args("psu set link flc-write-link        -writepref=20 -readpref=0 -cachepref=0" )  );
        _ci.command( new Args("psu set link hermes-write-link     -writepref=20 -readpref=0 -cachepref=0" )  );
        _ci.command( new Args("psu set link herab-write-link      -writepref=20 -readpref=0 -cachepref=0" )  );
        _ci.command( new Args("psu set link default-write-link-in -writepref=1  -readpref=0 -cachepref=0" )  );
        _ci.command( new Args("psu set link default-write-link-ex -writepref=1  -readpref=0 -cachepref=0" )  );


        // assign pool groups to links
        _ci.command( new Args("psu add link h1-read-link h1-read-pools" )  );
        _ci.command( new Args("psu add link h1-write-link h1-write-pools" )  );

        _ci.command( new Args("psu add link zeus-read-link zeus-read-pools" )  );
        _ci.command( new Args("psu add link zeus-write-link zeus-write-pools" )  );

        _ci.command( new Args("psu add link flc-read-link flc-read-pools" )  );
        _ci.command( new Args("psu add link flc-write-link flc-write-pools" )  );

        _ci.command( new Args("psu add link hermes-read-link hermes-read-pools" )  );
        _ci.command( new Args("psu add link hermes-write-link hermes-write-pools" )  );

        _ci.command( new Args("psu add link herab-read-link herab-read-pools" )  );
        _ci.command( new Args("psu add link herab-write-link herab-write-pools" )  );

        _ci.command( new Args("psu add link default-read-link-ex default-read-pools" )  );
        _ci.command( new Args("psu add link default-write-link-ex default-write-pools" )  );

        _ci.command( new Args("psu add link default-read-link-in default-read-pools" )  );
        _ci.command( new Args("psu add link default-write-link-in default-write-pools" )  );


    }

    /*
     * test case: check that if all pools is ofline, no pools returned
     */
    @Test
    public void testAllPoolsOffline() throws CommandException {


        _ci.command("psu set allpoolsactive off");


        PoolPreferenceLevel[] preference = _psu.match(
                                                      DirectionType.READ,  // operation
                                                      "*@*",   // storage unit
                                                      null,    // dCache unit
                                                      "131.169.214.149", // net unit
                                                      null,  // protocol
                                                      null,  // map
                                                      null); // linkGroup


        int found = 0;
        for (PoolPreferenceLevel level : preference) {
            found += level.getPoolList().size();
        }

        assertEquals(0,  found );

    }

    /*
     * test case: check that read with unknown storage group goes only to default-read pool
     */
    @Test
    public void testAnyRead() throws CommandException {


        _ci.command("psu set allpoolsactive on");


        PoolPreferenceLevel[] preference = _psu.match(
                                                      DirectionType.READ,  // operation
                                                      "*@*",   // storage unit
                                                      null,    // dCache unit
                                                      "131.169.214.149", // net unit
                                                      null,  // protocol
                                                      null,  // map
                                                      null); // linkGroup

        assertEquals("Only default read link have to be triggered", 1, preference.length);
        assertEquals("Only default read pool is allowed", 1, preference[0].getPoolList().size());
        assertEquals("Only default read pool is allowed (default-read)", "default-read", preference[0].getPoolList().get(0));
    }


    /*
     * test case: check that write with unknow storage group goes only to default-write pool
     */
    @Test
    public void testAnyWrite() throws CommandException {


        _ci.command("psu set allpoolsactive on");


        PoolPreferenceLevel[] preference = _psu.match(
                                                      DirectionType.WRITE,  // operation
                                                      "*@*",   // storage unit
                                                      null,    // dCache unit
                                                      "131.169.214.149", // net unit
                                                      null,  // protocol
                                                      null,  // map
                                                      null); // linkGroup

        assertEquals("Only default write link have to be triggered", 1, preference.length);
        assertEquals("Only default write pool is allowed", 1, preference[0].getPoolList().size());
        assertEquals("Only default write pool is allowed (default-read)", "default-write", preference[0].getPoolList().get(0));
    }

    /*
     * test case: check that write with to H1 return two pool h1-write(attraction 0) and default-write(attraction 1)
     */
    @Test
    public void testH1Write() throws CommandException {


        _ci.command("psu set allpoolsactive on");


        PoolPreferenceLevel[] preference = _psu.match(
                                                      DirectionType.WRITE,  // operation
                                                      "h1:u1@osm",   // storage unit
                                                      null,    // dCache unit
                                                      "131.169.214.149", // net unit
                                                      null,  // protocol
                                                      null,  // map
                                                      null); // linkGroup

        assertEquals("H1 write link and default write link have to be triggered", 2, preference.length);
        assertEquals("Only h1 write pool with attracion 0", 1, preference[0].getPoolList().size());
        assertEquals("Only h1 write pool with attracion 0 (h1-write)", "h1-write", preference[0].getPoolList().get(0));
    }

    /*
     * test case: check that read with to H1 return two pool h1-read(attraction 0) and default-read(attraction 1)
     */
    @Test
    public void testH1Read() throws CommandException {


        _ci.command("psu set allpoolsactive on");


        PoolPreferenceLevel[] preference = _psu.match(
                                                      DirectionType.READ,  // operation
                                                      "h1:u1@osm",   // storage unit
                                                      null,    // dCache unit
                                                      "131.169.214.149", // net unit
                                                      null,  // protocol
                                                      null,  // map
                                                      null); // linkGroup

        assertEquals("H1 read link and default read link have to be triggered", 2, preference.length);
        assertEquals("Only h1 read pool with attracion 0", 1, preference[0].getPoolList().size());
        assertEquals("Only h1 read pool with attracion 0 (h1-read)", "h1-read", preference[0].getPoolList().get(0));
    }

    /*
     * test case: check that if he pool is down, we get default pool
     */
    @Test
    public void testH1ReadFallback() throws CommandException {


        _ci.command("psu set allpoolsactive on");
        _ci.command("psu set disabled h1-read");


        PoolPreferenceLevel[] preference = _psu.match(
                                                      DirectionType.READ,  // operation
                                                      "h1:u1@osm",   // storage unit
                                                      null,    // dCache unit
                                                      "131.169.214.149", // net unit
                                                      null,  // protocol
                                                      null,  // map
                                                      null); // linkGroup

        assertEquals("H1 read link and default read link have to be triggered", 2, preference.length);
        assertEquals("No h1 pool when it's disabled with attracion 0", 0, preference[0].getPoolList().size());
        assertEquals("Only default read pool with attracion 0", 1, preference[1].getPoolList().size());
        assertEquals("Only default read pool with attracion 0 (default-read)", "default-read", preference[1].getPoolList().get(0));
    }


    /*

      @Test
      public void testSelectPoolWithoutGroup() throws CommandException {


      _ci.command("psu set allpoolsactive on");
      _ci.command(new Args("psu create linkGroup h1-link-group"));
      _ci.command(new Args("psu addto linkGroup h1-link-group h1-read-link"));


      PoolPreferenceLevel[] preference = _psu.match(
      "read",  // operation
      "h1:u1@osm",   // storage unit
      null,    // dCache unit
      "131.169.214.149", // net unit
      null,  // protocol
      null,  // map
      null); // linkGroup

      assertEquals("Only default read link have to be triggered", 1, preference.length);
      assertEquals("Only default read pool with attracion 0", 1, preference[0].getPoolList().size());
      assertEquals("Only default read pool with attracion 0 (default-read)", "default-read", preference[0].getPoolList().get(0));
      }
    */

    /*
     * test case: check that we do not get pools from LinkGroup
     */
    @Test
    public void testSelectPoolByLinkGroup() throws CommandException {


        _ci.command("psu set allpoolsactive on");
        _ci.command(new Args("psu create linkGroup h1-link-group"));
        _ci.command(new Args("psu addto linkGroup h1-link-group h1-read-link" ) );


        PoolPreferenceLevel[] preference = _psu.match(
                                                      DirectionType.READ,  // operation
                                                      "h1:u1@osm",   // storage unit
                                                      null,    // dCache unit
                                                      "131.169.214.149", // net unit
                                                      null,  // protocol
                                                      null,  // map
                                                      "h1-link-group"); // linkGroup

        assertEquals("Only h1 read link have to be triggered", 1, preference.length);
        assertEquals("Only h1 read pool with attracion 0", 1, preference[0].getPoolList().size());
        assertEquals("Only h1 read pool with attracion 0 (h1-read)", "h1-read", preference[0].getPoolList().get(0));
    }

    /*
     * test case: check that we do not get pools from LinkGroup
     */
    @Test
    public void testSelectStagePoolByLinkGroup() throws Exception {


        _ci.command("psu set allpoolsactive on");
        _ci.command(new Args("psu create linkGroup h1-link-group"));
        _ci.command(new Args("psu addto linkGroup h1-link-group h1-read-link" ) );

        StorageInfo storageInfo = new GenericStorageInfo("osm","h1:u1" );

        storageInfo.addLocation( new URI("osm://osm/?store=h1&bfid=1234") );

        Set<String> supportedHSM = new HashSet<String>();
        supportedHSM.add("osm");

        _psu.getPool("h1-read").setHsmInstances( supportedHSM );

        PoolPreferenceLevel[] preference = _psu.match(
                                                      DirectionType.CACHE,  // operation
                                                      "h1:u1@osm",   // storage unit
                                                      null,    // dCache unit
                                                      "131.169.214.149", // net unit
                                                      null,  // protocol
                                                      storageInfo,  // map
                                                      "h1-link-group"); // linkGroup

        assertEquals("Only h1 cache link have to be triggered", 1, preference.length);
        assertEquals("Only h1 cache pool with attracion 0", 1, preference[0].getPoolList().size());
        assertEquals("Only h1 cache pool with attracion 0 (h1-read)", "h1-read", preference[0].getPoolList().get(0));
    }


    /*
     * test case: check that we do not select read-only pools as p2p
     * destinations.
     */
    @Test
    public void testSelectForP2P() throws CommandException {

        _ci.command("psu set allpoolsactive on");
        _ci.command("psu set pool h1-read rdonly");

        PoolPreferenceLevel[] preference =
            _psu.match(DirectionType.P2P,  // operation
                       "h1:u1@osm",   // storage unit
                       null,    // dCache unit
                       "131.169.214.149", // net unit
                       null,  // protocol
                       null,  // map
                       null); // linkGroup

        List<String> pools = new ArrayList<String>();
        for(PoolPreferenceLevel level: preference) {
            pools.addAll( level.getPoolList() );
        }
        assertEquals("More than expected pools selected", 1, pools.size());
        assertEquals("Unexpected pool selected", "default-read", pools.get(0));
    }


    @Test
    public void testActive() throws CommandException {

        _ci.command( new Args("psu set active -on h1-read"  )  );
        SelectionPool pool = _psu.getPool("h1-read");

        assertNotNull("Null pool recieved", pool);
        assertTrue("Pool is not active", pool.isActive());
        assertTrue("Pool is not readable", pool.canRead());
    }
}
