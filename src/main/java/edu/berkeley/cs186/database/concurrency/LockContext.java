package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multi-granularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    private static final Logger LOG = LoggerFactory.getLogger(LockContext.class);

    /**
     * The underlying lock manager.
     */
    protected final LockManager lockManager;

    /**
     * The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
     */
    protected final LockContext parent;

    /**
     * The name of the resource this LockContext represents.
     */
    protected ResourceName name;

    /**
     * Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
     * throw an UnsupportedOperationException.
     */
    protected boolean readonly;

    /**
     * A mapping between transaction numbers, and the number of locks on children of this LockContext
     * that the transaction holds.
     */
    protected final Map<Long, Integer> numChildLocks;

    /**
     * You should not modify or use this directly.
     */
    protected final Map<String, LockContext> children;

    /**
     * Whether any new child LockContexts should be marked readonly.
     */
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockManager, LockContext parent, String name) {
        this(lockManager, parent, name, false);
    }

    protected LockContext(LockManager lockManager, LockContext parent, String name,
                          boolean readonly) {
        this.lockManager = lockManager;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockMgr, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockMgr.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     * <p>
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException          if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     *                                       transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement;
        panicWhenReadOnly();
        checkValidLock(transaction, lockType);
        lockManager.acquire(transaction, getResourceName(), lockType);
        final LockContext parentContext = parentContext();
        if (parentContext != null) {
            parentContext.updateChildLockNum(transaction, 1);
            LOG.debug(String.format("acquire lock %s===> parent %s update parent's child num to %s%n%n",
                    this, parent, parent.getNumChildren(transaction)));
        }
    }

    /**
     * update the number of the childLocks of this context
     * (update all of its ascendants recursively)
     *
     * @param txnCtx transaction
     * @param delta  add num
     */
    private void updateChildLockNum(TransactionContext txnCtx, int delta) {
        final long transNum = txnCtx.getTransNum();
//        numChildLocks.putIfAbsent(transNum, 0);
//        numChildLocks.computeIfPresent(transNum, (k, v) -> {
//            v += delta;
//            assert v >= 0;
//            return v;
//        });

//        numChildLocks.putIfAbsent(transNum, 0);
        numChildLocks.compute(transNum, (k, v) -> {
            if (v == null) {
                v = 0;
            }
            v += delta;
            return v;
        });
//        numChildLocks.put(transNum, numChildLocks.get(transNum) + delta);
        final LockContext parentContext = parentContext();
        LOG.debug("--->{} cNum {}\n", this, numChildLocks.get(transNum));
        if (parentContext != null) {
            parentContext.updateChildLockNum(txnCtx, delta);
        }
    }

    /**
     * Release `transaction`'s lock on `name`.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException           if no lock on `name` is held by `transaction`
     * @throws InvalidLockException          if the lock cannot be released because
     *                                       doing so would violate multi-granularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        panicWhenReadOnly();
        final LockContext parentContext = parentContext();
        if (getNumChildren(transaction) != 0) {
            throw new InvalidLockException("the release request is invalid");
        }
        lockManager.release(transaction, name);
        if (parentContext != null) {
            parentContext.updateChildLockNum(transaction, -1);
            LOG.debug(String.format("release lock %s===> parent %s update parent's child num to %s%n%n",
                    this, parent, parent.getNumChildren(transaction)));
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     *                                       `newLockType` lock
     * @throws NoLockHeldException           if `transaction` has no lock
     * @throws InvalidLockException          if the requested lock type is not a
     *                                       promotion or promoting would cause the lock manager to enter an invalid
     *                                       state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     *                                       type B is valid if B is substitutable for A and B is not equal to A, or
     *                                       if B is SIX and A is IX/IS/S, and invalid otherwise. hasSIXAncestor may
     *                                       be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement
        panicWhenReadOnly();
        checkValidLock(transaction, newLockType);
        final ResourceName resourceName = getResourceName();
        if (!Objects.equals(newLockType, LockType.SIX)) {
            lockManager.promote(transaction, resourceName, newLockType);
            return;
        }

        //deal with SIX type uniquely
        if (hasSIXAncestor(transaction)) {
            throw new InvalidLockException(String.format("%s got a SIX ancestor", this));
        }
        //use atomic release and acquire to act promote
        final List<ResourceName> toRelease = sisDescendants(transaction);
        updateChildLockNum(toRelease, transaction, -1);
        //caveat the order, self context still hold the lock
        toRelease.add(resourceName);
        LOG.debug(String.format("promote lock %s===> parent %s update parent's child num to %s%n%n",
                this, parent, parent.getNumChildren(transaction)));
        lockManager.acquireAndRelease(transaction, resourceName, newLockType, toRelease);
    }

    private void updateChildLockNum(List<ResourceName> toRelease, TransactionContext txnCtx, int delta) {
        toRelease.stream()
                .map(resourceName -> fromResourceName(lockManager, resourceName).parentContext())
                .filter(Objects::nonNull)
                .forEach(lockContext -> lockContext.updateChildLockNum(txnCtx, delta));
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     * <p>
     * For example, if a transaction has the following locks:
     * <p>
     * IX(database)
     * /          \
     * IX(table1)    S(table2)
     * /                 \
     * S(table1 page3)  X(table1 page5)
     * <p>
     * then after table1Context.escalate(transaction) is called, we should have:
     * <p>
     * IX(database)
     * /         \
     * X(table1)     S(table2)
     * <p>
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     * <p>
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException           if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement
        panicWhenReadOnly();
        final LockType prevLockType = getExplicitLockType(transaction);
        if (Objects.equals(prevLockType, LockType.NL)) {
            throw new NoLockHeldException(String.format("%s got no lock at this level", transaction));
        }
        Predicate<? super LockType> toXPredicate =
                lockType -> Objects.equals(lockType, LockType.X) ||
                        Objects.equals(lockType, LockType.IX) ||
                        Objects.equals(lockType, LockType.SIX);
        boolean isToX = toXPredicate.test(prevLockType);
        final List<ResourceName> descendants = getAllDescendants(transaction);
        if (descendants.isEmpty() && !prevLockType.isIntent()) {
            //do nothing
            return;
        }
        if (!isToX) {
            isToX = descendants.stream()
                    .map(resourceName -> lockManager.getLockType(transaction, resourceName))
                    .anyMatch(toXPredicate);
        }
        //to release list
        final ResourceName resourceName = getResourceName();
        descendants.add(resourceName);
        updateChildLockNum(descendants, transaction, -1);
        LOG.debug(String.format("escalate lock %s===> parent %s update parent's child num to %s%n%n",
                this, parent, parent.getNumChildren(transaction)));
        if (isToX) {
            lockManager.acquireAndRelease(transaction, resourceName, LockType.X, descendants);
        } else {
            lockManager.acquireAndRelease(transaction, resourceName, LockType.S, descendants);
        }
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        return lockManager.getLockType(transaction, getResourceName());
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) {
            return LockType.NL;
        }
        // TODO(proj4_part2): implement
        LockType lockType = getExplicitLockType(transaction);
        if (!Objects.equals(lockType, LockType.NL)) {
            return lockType;
        }
        final LockContext parentContext = parentContext();
        if (parentContext != null) {
            final LockType parentLockType = parentContext.getEffectiveLockType(transaction);
            if (Objects.equals(parentLockType, LockType.SIX)) {
                //parent SIX <=> children S
                lockType = LockType.S;
            } else if (!parentLockType.isIntent()) {
                //parent holds S/X <=> children hold S/X ()
                lockType = parentLockType;
            }
            //parent isIntent(except for SIX) <=> children NL
        }
        return lockType;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     *
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        for (LockContext ctx = parentContext(); ctx != null; ctx = ctx.parentContext()) {
            final LockType type = getEffectiveLockType(transaction);
            if (Objects.equals(type, LockType.SIX)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     *
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        return getEligibleDescendants(transaction,
                lock -> lock.name.isDescendantOf(getResourceName()) &&
                        lock.lockType == LockType.S ||
                        lock.lockType == LockType.IS);
    }

    private List<ResourceName> getAllDescendants(TransactionContext transaction) {
        return getEligibleDescendants(transaction,
                lock -> lock.name.isDescendantOf(getResourceName()));
    }

    private List<ResourceName> getEligibleDescendants(TransactionContext transaction,
                                                      Predicate<? super Lock> predicate) {
        return lockManager
                .getLocks(transaction)
                .stream()
                .filter(predicate)
                .map(lock -> lock.name)
                .collect(Collectors.toList());
    }

    private void panicWhenReadOnly() throws UnsupportedOperationException {
        if (readonly) {
            throw new UnsupportedOperationException(String.format("%s is readonly", this));
        }
    }

    private void checkValidLock(TransactionContext txnCtx, LockType lockType) throws InvalidLockException {
        final LockContext parentContext = parentContext();
        if (parentContext != null && !LockType.canBeParentLock(parentContext.getEffectiveLockType(txnCtx), lockType)) {
            throw new InvalidLockException(String.format("%s is invalid for parent %s", lockType, parentContext));
        }
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockManager, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

