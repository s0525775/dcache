package org.dcache.services.info.base;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

/**
 * This class provides a set of tests to check the behaviour of
 * {@link TestStateExhibitor}. Some of the tests involve the
 * {@link MalleableStateTransition} which, in turn, is tested using a
 * TestStateExhibitor, so not all tests are completely independent.
 */
public class TestStateExhibitorTests {

    TestStateExhibitor _exhibitor;
    static final StatePath METRIC_PATH = StatePath.parsePath( "aa.bb.metric value");

    @Before
    public void setUp() throws Exception {
        _exhibitor = new TestStateExhibitor();
    }

    @Test
    public void testEmptyExhibitor() {
        VerifyingVisitor visitor = new VerifyingVisitor();

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleStringMetricExhibitor() {
        assertSingleMetricOk( new StringStateValue( "a typical string value"));
    }

    @Test
    public void testSingleIntegerMetricExhibitor() {
        assertSingleMetricOk( new IntegerStateValue( 42));
    }

    @Test
    public void testSingleFloatingPointMetricExhibitor() {
        assertSingleMetricOk( new FloatingPointStateValue( 42.3));
    }

    @Test
    public void testSingleBooleanMetricExhibitor() {
        assertSingleMetricOk( new BooleanStateValue( true));
    }

    @Test
    public void testMultipleSiblingMetricExhibitor() {
        VerifyingVisitor visitor = new VerifyingVisitor();

        StatePath metricBranch = StatePath.parsePath( "aa.bb");

        StatePath metricPath1 = metricBranch.newChild( "metric 1");
        StatePath metricPath2 = metricBranch.newChild( "metric 2");

        StateValue metricValue1 = new StringStateValue(
                                                        "a typical string value");
        StateValue metricValue2 = new IntegerStateValue( 42);

        _exhibitor.addMetric( metricPath1, metricValue1);
        _exhibitor.addMetric( metricPath2, metricValue2);
        visitor.addExpectedMetric( metricPath1, metricValue1);
        visitor.addExpectedMetric( metricPath2, metricValue2);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleBranch() {
        StatePath branchPath = StatePath.parsePath( "aa.bb.cc");

        _exhibitor.addBranch( branchPath);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testTwoSiblingBranches() {
        StatePath branchPath1 = StatePath.parsePath( "aa.bb.cc.branch1");
        StatePath branchPath2 = StatePath.parsePath( "aa.bb.cc.branch2");

        _exhibitor.addBranch( branchPath1);
        _exhibitor.addBranch( branchPath2);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath1);
        visitor.addExpectedBranch( branchPath2);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testThreeSiblingBranches() {
        StatePath branchPath1 = StatePath.parsePath( "aa.bb.cc.branch1");
        StatePath branchPath2 = StatePath.parsePath( "aa.bb.cc.branch2");
        StatePath branchPath3 = StatePath.parsePath( "aa.bb.cc.branch3");

        _exhibitor.addBranch( branchPath1);
        _exhibitor.addBranch( branchPath2);
        _exhibitor.addBranch( branchPath3);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath1);
        visitor.addExpectedBranch( branchPath2);
        visitor.addExpectedBranch( branchPath3);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testFourSiblingBranches() {
        StatePath branchPath1 = StatePath.parsePath( "aa.bb.cc.branch1");
        StatePath branchPath2 = StatePath.parsePath( "aa.bb.cc.branch2");
        StatePath branchPath3 = StatePath.parsePath( "aa.bb.cc.branch3");
        StatePath branchPath4 = StatePath.parsePath( "aa.bb.cc.branch4");

        _exhibitor.addBranch( branchPath1);
        _exhibitor.addBranch( branchPath2);
        _exhibitor.addBranch( branchPath3);
        _exhibitor.addBranch( branchPath4);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath1);
        visitor.addExpectedBranch( branchPath2);
        visitor.addExpectedBranch( branchPath3);
        visitor.addExpectedBranch( branchPath4);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testFourSiblingBranchesWithSubbranches() {
        StatePath branchPath1 = StatePath.parsePath( "aa.bb.cc.branch1");
        StatePath branchPath2a = StatePath.parsePath( "aa.bb.cc.branch2.a");
        StatePath branchPath2b = StatePath.parsePath( "aa.bb.cc.branch2.b");
        StatePath branchPath3 = StatePath.parsePath( "aa.bb.cc.branch3");
        StatePath branchPath3a = StatePath.parsePath( "aa.bb.cc.branch3.a");
        StatePath branchPath3b = StatePath.parsePath( "aa.bb.cc.branch3.b");
        StatePath branchPath3c = StatePath.parsePath( "aa.bb.cc.branch3.c");
        StatePath branchPath4 = StatePath.parsePath( "aa.bb.cc.branch4");

        _exhibitor.addBranch( branchPath1);
        _exhibitor.addBranch( branchPath2a);
        _exhibitor.addBranch( branchPath2b);
        _exhibitor.addBranch( branchPath3);
        _exhibitor.addBranch( branchPath3a);
        _exhibitor.addBranch( branchPath3b);
        _exhibitor.addBranch( branchPath3c);
        _exhibitor.addBranch( branchPath4);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( branchPath1);
        visitor.addExpectedBranch( branchPath2a);
        visitor.addExpectedBranch( branchPath2b);
        visitor.addExpectedBranch( branchPath3);
        visitor.addExpectedBranch( branchPath3a);
        visitor.addExpectedBranch( branchPath3b);
        visitor.addExpectedBranch( branchPath3c);
        visitor.addExpectedBranch( branchPath4);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testRealisticExample() {
        StatePath link1Pool1 = StatePath.parsePath( "links.link-1.pools.pool1");
        StatePath link1Pool2 = StatePath.parsePath( "links.link-1.pools.pool2");
        StatePath link1ReadPrefPath = StatePath.parsePath( "links.link-1.prefs.read");
        StateValue link1ReadPrefMetric = new IntegerStateValue( 5);
        StatePath link1WritePrefPath = StatePath.parsePath( "links.link-1.prefs.write");
        StateValue link1WritePrefMetric = new IntegerStateValue( 7);
        StatePath link1P2pPrefPath = StatePath.parsePath( "links.link-1.prefs.p2p");
        StateValue link1P2pPrefMetric = new IntegerStateValue( 11);
        StatePath link1CachePrefPath = StatePath.parsePath( "links.link-1.prefs.cache");
        StateValue link1CachePrefMetric = new IntegerStateValue( 13);

        _exhibitor.addBranch( link1Pool1);
        _exhibitor.addBranch( link1Pool2);
        _exhibitor.addMetric( link1ReadPrefPath, link1ReadPrefMetric);
        _exhibitor.addMetric( link1WritePrefPath, link1WritePrefMetric);
        _exhibitor.addMetric( link1P2pPrefPath, link1P2pPrefMetric);
        _exhibitor.addMetric( link1CachePrefPath, link1CachePrefMetric);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( link1Pool1);
        visitor.addExpectedBranch( link1Pool2);
        visitor.addExpectedMetric( link1ReadPrefPath, link1ReadPrefMetric);
        visitor.addExpectedMetric( link1WritePrefPath, link1WritePrefMetric);
        visitor.addExpectedMetric( link1P2pPrefPath, link1P2pPrefMetric);
        visitor.addExpectedMetric( link1CachePrefPath, link1CachePrefMetric);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    /**
     * TESTS INVOLVING CLONED Node STRUCTURE
     */

    @Test
    public void testSingleStringMetricClonedExhibitor() {
        assertSingleMetricClonedOk( new StringStateValue(
                                                          "a typical string value"));
    }

    @Test
    public void testSingleIntegerMetricClonedExhibitor() {
        assertSingleMetricClonedOk( new IntegerStateValue( 42));
    }

    @Test
    public void testSingleFloatingPointMetricClonedExhibitor() {
        assertSingleMetricClonedOk( new FloatingPointStateValue( 42.3));
    }

    @Test
    public void testSingleBooleanMetricClonedExhibitor() {
        assertSingleMetricClonedOk( new BooleanStateValue( true));
    }

    @Test
    public void testClonedRealisticExample() {
        StatePath link1Pool1 = StatePath.parsePath( "links.link-1.pools.pool1");
        StatePath link1Pool2 = StatePath.parsePath( "links.link-1.pools.pool2");
        StatePath link1ReadPrefPath = StatePath.parsePath( "links.link-1.prefs.read");
        StateValue link1ReadPrefMetric = new IntegerStateValue( 5);
        StatePath link1WritePrefPath = StatePath.parsePath( "links.link-1.prefs.write");
        StateValue link1WritePrefMetric = new IntegerStateValue( 7);
        StatePath link1P2pPrefPath = StatePath.parsePath( "links.link-1.prefs.p2p");
        StateValue link1P2pPrefMetric = new IntegerStateValue( 11);
        StatePath link1CachePrefPath = StatePath.parsePath( "links.link-1.prefs.cache");
        StateValue link1CachePrefMetric = new IntegerStateValue( 13);

        _exhibitor.addBranch( link1Pool1);
        _exhibitor.addBranch( link1Pool2);
        _exhibitor.addMetric( link1ReadPrefPath, link1ReadPrefMetric);
        _exhibitor.addMetric( link1WritePrefPath, link1WritePrefMetric);
        _exhibitor.addMetric( link1P2pPrefPath, link1P2pPrefMetric);
        _exhibitor.addMetric( link1CachePrefPath, link1CachePrefMetric);

        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedBranch( link1Pool1);
        visitor.addExpectedBranch( link1Pool2);
        visitor.addExpectedMetric( link1ReadPrefPath, link1ReadPrefMetric);
        visitor.addExpectedMetric( link1WritePrefPath, link1WritePrefMetric);
        visitor.addExpectedMetric( link1P2pPrefPath, link1P2pPrefMetric);
        visitor.addExpectedMetric( link1CachePrefPath, link1CachePrefMetric);

        _exhibitor.visitClonedState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    /**
     * TESTS INVOLVING TRANSITIONS
     */

    @Test
    public void testSingleStringMetricTransitionExhibitor() {
        assertSingleMetricTransitionOk( new StringStateValue(
                                                              "a typical string value"));
    }

    @Test
    public void testSingleIntegerMetricTransitionExhibitor() {
        assertSingleMetricTransitionOk( new IntegerStateValue( 42));
    }

    @Test
    public void testSingleFloatingPointMetricTransitionExhibitor() {
        assertSingleMetricTransitionOk( new FloatingPointStateValue( 42.3));
    }

    @Test
    public void testSingleBooleanMetricTransitionExhibitor() {
        assertSingleMetricTransitionOk( new BooleanStateValue( true));
    }

    /**
     * TESTS INVOLVING SKIPPING
     */

    @Test
    public void testSingleMetricSkipNull() {
        StatePath metricPath = StatePath.parsePath( "aa.bb.metric");
        StateValue metricValue = new StringStateValue( "some string");

        _exhibitor.addMetric( metricPath, metricValue);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);

        _exhibitor.visitState( visitor, (StatePath) null);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleMetricSkipOneElement() {
        StatePath metricPath = StatePath.parsePath( "aa.bb.metric");
        StateValue metricValue = new StringStateValue( "some string");
        StatePath skipPath = StatePath.parsePath( "aa");

        _exhibitor.addMetric( metricPath, metricValue);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleMetricSkipTwoElements() {
        StatePath metricPath = StatePath.parsePath( "aa.bb.metric");
        StateValue metricValue = new StringStateValue( "some string");
        StatePath skipPath = StatePath.parsePath( "aa.bb");

        _exhibitor.addMetric( metricPath, metricValue);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleMetricSkipAllElements() {
        StatePath metricPath = StatePath.parsePath( "aa.bb.metric");
        StateValue metricValue = new StringStateValue( "some string");
        StatePath skipPath = StatePath.parsePath( "aa.bb.metric");

        _exhibitor.addMetric( metricPath, metricValue);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleBranchSkipOneElement() {
        StatePath skipPath = StatePath.parsePath( "aa");
        StatePath branchPath = skipPath.newChild( StatePath.parsePath( "list.list-item"));

        _exhibitor.addBranch( branchPath);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( branchPath);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    @Test
    public void testSingleBranchSkipTwoElement() {
        StatePath skipPath = StatePath.parsePath( "aa.list");
        StatePath branchPath = skipPath.newChild( StatePath.parsePath( "list-item"));

        _exhibitor.addBranch( branchPath);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( branchPath);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TESTS INVOLVING SKIPPING AND TRANSITION
     */

    /**
     * TestStateExhibitor has a single metric. The StateTransition does
     * nothing. We skip nothing.
     */
    @Test
    public void testSingleMetricTransitionEmptyAndSkipNull() {
        StatePath metricPath = StatePath.parsePath( "aa.bb.metric");
        StateValue metricValue = new StringStateValue( "some string");

        _exhibitor.addMetric( metricPath, metricValue);

        MalleableStateTransition transition = new MalleableStateTransition();

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);

        _exhibitor.visitState( transition, visitor, null);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor has a single metric. The StateTransition does
     * nothing. We skip the first path element.
     */
    @Test
    public void testSingleMetricTransitionEmptySkipOneElement() {
        StatePath skipPath = StatePath.parsePath( "aa");
        StatePath metricPath = skipPath.newChild( StatePath.parsePath( "bb.metric"));
        StateValue metricValue = new StringStateValue( "some string");

        _exhibitor.addMetric( metricPath, metricValue);

        MalleableStateTransition transition = new MalleableStateTransition();

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor has a single metric. The StateTransition does
     * nothing. We skip two path elements.
     */
    @Test
    public void testSingleMetricTransitionEmptySkipTwoElements() {
        StatePath skipPath = StatePath.parsePath( "aa.bb");
        StatePath metricPath = skipPath.newChild( "metric");
        StateValue metricValue = new StringStateValue( "some string");

        _exhibitor.addMetric( metricPath, metricValue);

        MalleableStateTransition transition = new MalleableStateTransition();

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor has a single metric. The StateTransition does
     * nothing. We skip all path elements.
     */
    @Test
    public void testSingleMetricTransitionEmptySkipAllElements() {
        StatePath skipPath = StatePath.parsePath( "aa.bb.metric");
        StatePath metricPath = new StatePath( skipPath);
        StateValue metricValue = new StringStateValue( "some string");

        _exhibitor.addMetric( metricPath, metricValue);

        MalleableStateTransition transition = new MalleableStateTransition();

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor has a single branch. The StateTransition does
     * nothing. We the first path element.
     */
    @Test
    public void testSingleBranchTransitionEmptySkipOneElement() {
        StatePath skipPath = StatePath.parsePath( "aa");
        StatePath branchPath = skipPath.newChild( StatePath.parsePath( "list.list-item"));

        _exhibitor.addBranch( branchPath);

        MalleableStateTransition transition = new MalleableStateTransition();

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( branchPath);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor has a single branch. The StateTransition does
     * nothing. We two path elements.
     */
    @Test
    public void testSingleBranchTransitionEmptySkipTwoElement() {
        StatePath skipPath = StatePath.parsePath( "aa.list");
        StatePath branchPath = skipPath.newChild( StatePath.parsePath( "list-item"));

        _exhibitor.addBranch( branchPath);

        MalleableStateTransition transition = new MalleableStateTransition();

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( branchPath);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor is empty. The StateTransition adds a single metric.
     * We skip nothing.
     */
    @Test
    public void testEmptyTransitionAddSingleMetricSkipNull() {
        StatePath metricPath = StatePath.parsePath( "aa.bb.metric");
        StateValue metricValue = new StringStateValue( "some string");

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewMetric( metricPath, metricValue, 0);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);

        _exhibitor.visitState( transition, visitor, null);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor is empty. The StateTransition adds a single metric.
     * We skip the first path element.
     */
    @Test
    public void testEmptyTransitionAddSingleMetricSkipOneElement() {
        StatePath skipPath = StatePath.parsePath( "aa");
        StatePath metricPath = skipPath.newChild( StatePath.parsePath( "bb.metric"));
        StateValue metricValue = new StringStateValue( "some string");

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewMetric( metricPath, metricValue, 0);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor is empty. The StateTransition adds a single metric.
     * We skip two path elements.
     */
    @Test
    public void testEmptyTransitionAddSingleMetricSkipTwoElements() {
        StatePath skipPath = StatePath.parsePath( "aa.bb");
        StatePath metricPath = skipPath.newChild( "metric");
        StateValue metricValue = new StringStateValue( "some string");

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewMetric( metricPath, metricValue, 0);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor is empty. The StateTransition adds a single metric.
     * We skip all path elements.
     */
    @Test
    public void testEmptyTransitionAddSingleMetricSkipAllElements() {
        StatePath skipPath = StatePath.parsePath( "aa.bb.metric");
        StatePath metricPath = new StatePath( skipPath);
        StateValue metricValue = new StringStateValue( "some string");

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewMetric( metricPath, metricValue, 0);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( metricPath, metricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor is empty. The StateTransition adds a single branch.
     * We skip first path element.
     */
    @Test
    public void testEmptyTransitionAddsSingleBranchSkipOneElement() {
        StatePath skipPath = StatePath.parsePath( "aa");
        StatePath branchPath = skipPath.newChild( StatePath.parsePath( "list.list-item"));

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewBranch( branchPath, 0);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( branchPath);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor is empty. The StateTransition adds a single branch.
     * We skip two path elements.
     */
    @Test
    public void testEmptyStateTransitionAddsSingleBranchSkipTwoElement() {
        StatePath skipPath = StatePath.parsePath( "aa.list");
        StatePath branchPath = skipPath.newChild( StatePath.parsePath( "list-item"));

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewBranch( branchPath, 0);

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedBranch( branchPath);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor has a single metric. The StateTransition adds a
     * sibling metric, we skip none.
     */
    @Test
    public void testSingleMetricTransitionAddsSiblingSingleMetricSkipNull() {
        StatePath metricBranchPath = StatePath.parsePath( "aa.bb");
        StatePath existingMetricPath = metricBranchPath.newChild( "metric1");
        StateValue existingMetricValue = new StringStateValue( "metric1 value");
        StatePath newMetricPath = metricBranchPath.newChild( "metric2");
        StateValue newMetricValue = new StringStateValue( "metric2 value");

        _exhibitor.addMetric( existingMetricPath, existingMetricValue);

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewMetric(
                                                 newMetricPath,
                                                 newMetricValue,
                                                 metricBranchPath._elements.size());

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( existingMetricPath, existingMetricValue);
        visitor.addExpectedMetric( newMetricPath, newMetricValue);

        _exhibitor.visitState( transition, visitor, null);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /**
     * TestStateExhibitor has a single metric. The StateTransition adds a
     * sibling metric, we skip the first branch.
     */
    @Test
    public void testSingleMetricTransitionAddsSiblingSingleMetricSkipOneElement() {

        StatePath skipPath = StatePath.parsePath( "aa");
        StatePath metricBranchPath = skipPath.newChild( "bb");
        StatePath existingMetricPath = metricBranchPath.newChild( "metric1");
        StateValue existingMetricValue = new StringStateValue( "metric1 value");
        StatePath newMetricPath = metricBranchPath.newChild( "metric2");
        StateValue newMetricValue = new StringStateValue( "metric2 value");

        _exhibitor.addMetric( existingMetricPath, existingMetricValue);

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewMetric(
                                                 newMetricPath,
                                                 newMetricValue,
                                                 metricBranchPath._elements.size());

        VerifyingVisitor visitor = new VerifyingVisitor();
        visitor.addExpectedMetric( existingMetricPath, existingMetricValue);
        visitor.addExpectedMetric( newMetricPath, newMetricValue);
        visitor.setSkip( skipPath);

        _exhibitor.visitState( transition, visitor, skipPath);

        assertTrue( "visitor is satisfied", visitor.satisfied());
    }

    /*
     * PRIVATE SUPPORT METHODS.
     */

    /**
     * Add a single metric to the {@link TestStateExhibitor} and configure
     * the {@link VerifyingVisitor} so it expects this metric at the same
     * location.
     */
    private void addSingleMetricAndUpdateVisitor( VerifyingVisitor visitor,
                                                  StateValue metricValue) {
        _exhibitor.addMetric( METRIC_PATH, metricValue);
        visitor.addExpectedMetric( METRIC_PATH, metricValue);
    }

    /**
     * Add a single metric to an empty {@link TestStateExhibitor}. An empty
     * {@link VerifyingVisitor} is created and told to expect a metric at the
     * same location. The state is visited using
     * {@link TestStateExhibitor#visitState(StateVisitor)} and the
     * VerifyingVisitor is checked to see whether it is satisfied with the
     * result.
     * 
     * @param metricValue
     *            the metric value to check.
     * @param useClone
     *            if true then visitClonedState() is used instead of
     *            visitState()
     */
    private void assertSingleMetricOk( StateValue metricValue) {
        VerifyingVisitor visitor = new VerifyingVisitor();

        addSingleMetricAndUpdateVisitor( visitor, metricValue);

        _exhibitor.visitState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    /**
     * Similar to {@link #assertSingleMetricOk} but visit the state using
     * {@link TestStateExhibitor#visitClonedState(StateVisitor)}
     * 
     * @param metricValue
     */
    private void assertSingleMetricClonedOk( StateValue metricValue) {
        VerifyingVisitor visitor = new VerifyingVisitor();

        addSingleMetricAndUpdateVisitor( visitor, metricValue);

        _exhibitor.visitClonedState( visitor);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

    /**
     * Create a new {@link MalleableStateTransition}. Update this
     * MalleableStateTransition so the transition will add a single metric to
     * a predefined location. Create a new {@link VerifyingVisitor}, which is
     * told to expect the same metric at the same location. Visit the state
     * after the transition using the
     * {@link TestStateExhibitor#visitState(StateVisitor, StateTransition)}
     * method. Check that the VerifyingVisitor is satisfied.
     * 
     * @param metricValue
     */
    private void assertSingleMetricTransitionOk( StateValue metricValue) {
        VerifyingVisitor visitor = new VerifyingVisitor();

        visitor.addExpectedMetric( METRIC_PATH, metricValue);

        MalleableStateTransition transition = new MalleableStateTransition();
        transition.updateTransitionForNewMetric( METRIC_PATH, metricValue, 0);

        _exhibitor.visitState( visitor, transition);

        assertTrue( "VerifyingVisitor is satisfied", visitor.satisfied());
    }

}