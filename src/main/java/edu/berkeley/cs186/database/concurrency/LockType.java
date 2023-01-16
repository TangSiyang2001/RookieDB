package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    /**
     * 锁类型
     */
    NL,  // no lock held
    IS,  // intention shared
    IX,  // intention exclusive
    S,   // shared
    SIX, // shared intention exclusive
    X;   // exclusive

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     * <p>
     * <p>
     * | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  T  |  T  |  T  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |  T  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        //according to the compatible matrix
        if (a.ordinal() + b.ordinal() >= X.ordinal() + 1) {
            return S.equals(a) && S.equals(b);
        }
        return !(IX.equals(a) && S.equals(b) || S.equals(a) && IX.equals(b));
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S:
            case IS:
                return IS;
            case X:
            case IX:
            case SIX:
                return IX;
            case NL:
                return NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     * <p>
     * Parent Matrix
     * (Boolean value in cell answers can `left` be the parent of `top`?)
     * <p>
     *     | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |  T  |  T  |  F  |  T  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |  T  |  T  |  T  |  T  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * S   |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |  T  |  F  |  T  |  F  |  T  |  T
     * ----+-----+-----+-----+-----+-----+-----
     * X   |  T  |   F |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (parentLockType) {
            case IS:
                return NL.equals(childLockType) || IS.equals(childLockType) || S.equals(childLockType);
            case IX:
                return true;
            case SIX:
                return NL.equals(childLockType) || X.equals(childLockType) || IX.equals(childLockType)
                        || SIX.equals(childLockType);
            case NL:
            case S:
            case X:
                return NL.equals(childLockType);
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     * <p>
     * | NL  | IS  | IX  |  S  | SIX |  X
     * ----+-----+-----+-----+-----+-----+-----
     * NL  |  T  |  F  |  F  |  F  |  F  |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IS  |     |  T  |  F  |  F  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     * IX  |     |  T  |  T  |  F  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     * S   |     |     |     |  T  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     * SIX |     |     |     |  T  |     |  F
     * ----+-----+-----+-----+-----+-----+-----
     * X   |     |     |     |  T  |     |  T
     * ----+-----+-----+-----+-----+-----+-----
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(proj4_part1): implement
        switch (substitute) {
            case NL:
                return NL.equals(required);
            case IS:
                return NL.equals(required) || IS.equals(required);
            case IX:
                return NL.equals(required) || IS.equals(required) || IX.equals(required);
            case X:
                return true;
            case S:
                return NL.equals(required) || IS.equals(required) || S.equals(required);
            case SIX:
                return !X.equals(required);
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
            case S:
                return "S";
            case X:
                return "X";
            case IS:
                return "IS";
            case IX:
                return "IX";
            case SIX:
                return "SIX";
            case NL:
                return "NL";
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }
}

