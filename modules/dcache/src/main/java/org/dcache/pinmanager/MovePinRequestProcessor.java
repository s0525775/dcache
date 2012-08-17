package org.dcache.pinmanager;

import java.util.Date;
import java.util.UUID;
import java.util.Collection;
import static java.util.concurrent.TimeUnit.*;
import javax.security.auth.Subject;

import org.dcache.cells.CellStub;
import org.dcache.cells.CellMessageReceiver;
import org.dcache.auth.Subjects;
import org.dcache.pinmanager.model.Pin;
import org.dcache.services.pinmanager1.PinManagerMovePinMessage;
import org.dcache.pool.repository.StickyRecord;

import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import dmg.cells.nucleus.CellPath;

import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.annotation.Transactional;

import static org.dcache.pinmanager.model.Pin.State.*;
import static org.springframework.transaction.annotation.Isolation.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Processes requests to move pins.
 *
 * The stratetegy for moving a pin is the following:
 *
 *  1. Create a record in the DB for the target pool
 *  2. Create a sticky flag on the target pool
 *  3. Change the original record to point to the target pool and
 *     change the record created in step 1 to point to the old pool.
 *  4. Remove the sticky flag from the old pool.
 *
 * If the above process is aborted at any point the regular recovery
 * tasks of the pin manager will remove any stale sticky flag from
 * either the old or the new pool.
 *
 * Pin lifetime extension is treated as a special case of move (moving
 * to the same pool, but with a longer lifetime).
 */
public class MovePinRequestProcessor
    implements CellMessageReceiver
{
    private final static Logger _log =
        LoggerFactory.getLogger(MovePinRequestProcessor.class);

    private final static long POOL_LIFETIME_MARGIN = MINUTES.toMillis(30);

    private PinDao _dao;
    private CellStub _poolStub;
    private AuthorizationPolicy _pdp;
    private long _maxLifetime;

    @Required
    public void setDao(PinDao dao)
    {
        _dao = dao;
    }

    @Required
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    @Required
    public void setAuthorizationPolicy(AuthorizationPolicy pdp)
    {
        _pdp = pdp;
    }

    @Required
    public void setMaxLifetime(long maxLifetime)
    {
        _maxLifetime = maxLifetime;
    }

    @Required
    public long getMaxLifetime()
    {
        return _maxLifetime;
    }

    protected Pin createTemporaryPin(PnfsId pnfsId, String pool)
        throws CacheException
    {
        long now = System.currentTimeMillis();
        Pin pin = new Pin(Subjects.ROOT, pnfsId);
        pin.setState(PINNING);
        pin.setPool(pool);
        pin.setSticky("PinManager-" + UUID.randomUUID().toString());
        pin.setExpirationTime(new Date(now + 2 * _poolStub.getTimeout()));
        return _dao.storePin(pin);
    }

    @Transactional(isolation=REPEATABLE_READ)
    protected Pin swapPins(Pin pin, Pin tmpPin, Date expirationTime)
        throws CacheException
    {
        Pin targetPin =
            _dao.getPin(tmpPin.getPinId(), tmpPin.getSticky(), PINNING);
        if (targetPin == null) {
            /* The pin likely expired. We are now in a situation in
             * which we may or may not have a sticky flag on the
             * target pool, but no record in the database. To be on
             * the safe side we create a new record in the database
             * and then abort.
             */
            targetPin = new Pin(Subjects.ROOT, pin.getPnfsId());
            targetPin.setPool(tmpPin.getPool());
            targetPin.setSticky(tmpPin.getSticky());
            targetPin.setState(UNPINNING);
            _dao.storePin(targetPin);
            throw new TimeoutCacheException("Move expired");
        }

        Pin sourcePin =
            _dao.getPin(pin.getPinId(), pin.getSticky(), PINNED);
        if (sourcePin == null) {
            /* The target pin will expire by itself.
             */
            throw new CacheException("Pin no longer valid");
        }

        sourcePin.setPool(targetPin.getPool());
        sourcePin.setSticky(targetPin.getSticky());
        sourcePin.setExpirationTime(expirationTime);
        _dao.storePin(sourcePin);

        targetPin.setPool(pin.getPool());
        targetPin.setSticky(pin.getSticky());
        targetPin.setState(UNPINNING);
        return _dao.storePin(targetPin);
    }

    private void setSticky(String pool, PnfsId pnfsId, boolean sticky, String owner, long validTill)
        throws CacheException, InterruptedException
    {
        PoolSetStickyMessage msg =
            new PoolSetStickyMessage(pool, pnfsId, sticky, owner, validTill);
        _poolStub.sendAndWait(new CellPath(pool), msg);
    }

    protected Pin move(Pin pin, String pool, Date expirationTime)
        throws CacheException, InterruptedException
    {
        Pin tmpPin = createTemporaryPin(pin.getPnfsId(), pool);
        setSticky(tmpPin.getPool(),
                  tmpPin.getPnfsId(),
                  true,
                  tmpPin.getSticky(),
                  (expirationTime == null) ? - 1 : (expirationTime.getTime() + POOL_LIFETIME_MARGIN));
        return swapPins(pin, tmpPin, expirationTime);
    }

    private boolean containsPin(Collection<Pin> pins, String sticky)
    {
        for (Pin pin: pins) {
            if (sticky.equals(pin.getSticky())) {
                return true;
            }
        }
        return false;
    }

    public PinManagerMovePinMessage
        messageArrived(PinManagerMovePinMessage message)
        throws CacheException, InterruptedException
    {
        PnfsId pnfsId = message.getPnfsId();
        String source = message.getSourcePool();
        String target = message.getTargetPool();

        Collection<Pin> pins = _dao.getPins(pnfsId, source);

        /* Remove all stale sticky flags.
         */
        for (StickyRecord record: message.getRecords()) {
            if (!containsPin(pins, record.owner())) {
                setSticky(source, pnfsId, false, record.owner(), 0);
            }
        }

        /* Move all pins to the target pool.
         */
        for (Pin pin: pins) {
            Pin tmpPin = move(pin, target, pin.getExpirationTime());

            /**
             * For purposes of migrating from the previous PinManager
             * we need to deal with sticky flags shared by multiple
             * pins. We will not remove a sticky flag as long as there
             * are other pins using it.
             */
            if (!_dao.hasSharedSticky(tmpPin)) {
                setSticky(tmpPin.getPool(),
                          tmpPin.getPnfsId(),
                          false,
                          tmpPin.getSticky(),
                          0);
                _dao.deletePin(tmpPin);
            }
        }

        _log.info("Moved pins for {} from {} to {}",
                  new Object[] { pnfsId, source, target });

        return message;
    }

    public PinManagerExtendPinMessage
        messageArrived(PinManagerExtendPinMessage message)
        throws CacheException, InterruptedException
    {
        Pin pin = _dao.getPin(message.getFileAttributes().getPnfsId(),
                              message.getPinId());
        if (pin == null) {
            throw new InvalidMessageCacheException("Pin does not exist");
        } else if (!_pdp.canExtend(message.getSubject(), pin)) {
            throw new PermissionDeniedCacheException("Access denied");
        } else if (pin.getState() == PINNING) {
            throw new InvalidMessageCacheException("File is not pinned yet");
        } else if (pin.getState() == UNPINNING) {
            throw new InvalidMessageCacheException("Pin is no longer valid");
        }

        if (_maxLifetime > -1) {
            message.setLifetime(Math.min(_maxLifetime, message.getLifetime()));
        }

        long lifetime = message.getLifetime();
        if (pin.hasRemainingLifetimeLessThan(lifetime)) {
            long now = System.currentTimeMillis();
            Date date = (lifetime == -1) ? null : new Date(now + lifetime);
            move(pin, pin.getPool(), date);
            message.setExpirationTime(date);
        } else {
            message.setExpirationTime(pin.getExpirationTime());
        }

        _log.info("Extended pin for {} ({})", pin.getPnfsId(), pin.getPinId());

        return message;
    }
}