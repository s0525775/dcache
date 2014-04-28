package org.dcache.poolmanager;

import com.google.common.base.Functions;
import org.junit.Test;

import java.util.Collections;

import diskCacheV111.pools.PoolCostInfo;

import static java.util.Arrays.asList;
import static java.util.Collections.*;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class WeightedAvailableSpaceSelectionTest
{
    private final WeightedAvailableSpaceSelection wass =
        new WeightedAvailableSpaceSelection(1.0, 1.0);

    public void checkAvailable(long expected,
                               long free,
                               long removable,
                               double breakeven,
                               long lru,
                               long gap)
    {
        PoolCostInfo info = new PoolCostInfo("pool");
        info.setSpaceUsage(free + removable, free, 0, removable, lru);
        info.getSpaceInfo().setParameter(breakeven, gap);
        assertEquals(expected, (long) wass.getAvailable(info.getSpaceInfo(), 0));
    }

    @Test
    public void testNoRemovable()
    {
        checkAvailable(1000000, 1000000, 0, 0.5, 1000, 1000);
    }

    @Test
    public void testNoRemovableLessThanGap()
    {
        checkAvailable(0, 1000000, 0, 0.5, 1000, 1000000);
    }

    @Test
    public void testBreakEvenZero()
    {
        checkAvailable(1500000, 1000000, 500000, 0.0, 1000, 1000);
    }

    @Test
    public void testBreakLruZero()
    {
        checkAvailable(1000000, 1000000, 500000, 0.5, 0, 1000);
    }

    @Test
    public void testLruOneWeekHalflifeOneWeek()
    {
        checkAvailable((long) (1000000 + 500000 - 500000 * 0.5 / Math.log(2)),
                       1000000, 500000, 0.5, (7 * 24 * 3600), 1000);
    }

    @Test
    public void testLruOneWeekHalflifeOneWeekLessThanGap()
    {
        checkAvailable(0, 1000000, 500000, 0.5, (7 * 24 * 3600), 1500000);
    }

    @Test
    public void testLargeLoad()
    {
        PoolCostInfo info = new PoolCostInfo("pool");
        info.setSpaceUsage(100000000, 100000000, 0, 0);
        info.getSpaceInfo().setParameter(0, 1000);
        info.setMoverCostFactor(0.5);
        info.addExtendedMoverQueueSizes("movers", 3000, 3000, 0, 0, 3000);
        PoolCostInfo selected =
                wass.selectByAvailableSpace(singletonList(info), 1000,
                                            Functions.<PoolCostInfo>identity());
        assertThat(selected, is(info));
    }

    @Test
    public void testIdleFullPoolDoesNotAffectLoadNormalization()
    {
        PoolCostInfo busy = new PoolCostInfo("pool1");
        busy.setSpaceUsage(100000000, 100000000, 0, 0);
        busy.getSpaceInfo().setParameter(0, 1000);
        busy.setMoverCostFactor(0.5);
        busy.addExtendedMoverQueueSizes("movers", 3000, 3000, 0, 0, 3000);
        PoolCostInfo veryBusy = new PoolCostInfo("pool2");
        veryBusy.setSpaceUsage(100000000, 100000000, 0, 0);
        veryBusy.getSpaceInfo().setParameter(0, 1000);
        veryBusy.setMoverCostFactor(0.5);
        veryBusy.addExtendedMoverQueueSizes("movers", 6000, 6000, 0, 0, 6000);
        PoolCostInfo full = new PoolCostInfo("pool3");
        full.setSpaceUsage(100000000, 0, 100000000, 0);
        full.getSpaceInfo().setParameter(0, 1000);
        full.setMoverCostFactor(0.5);
        full.addExtendedMoverQueueSizes("movers", 0, 3000, 0, 0, 0);
        PoolCostInfo selected =
                wass.selectByAvailableSpace(asList(full, busy, veryBusy), 1000,
                                            Functions.<PoolCostInfo>identity());
        assertThat(selected, is(busy));
    }
}
