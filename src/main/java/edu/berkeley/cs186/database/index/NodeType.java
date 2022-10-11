package edu.berkeley.cs186.database.index;

/**
 * @author Steven.T
 * @date 2022/10/2
 */
public enum NodeType {

    /**
     * 0->inner
     * 1->leaf
     */
    INNER((byte) 0x00),
    LEAF((byte) 0x01);

    /**
     * represent the node type
     */
    private final byte type;

    NodeType(byte type) {
        this.type = type;
    }

    public byte getType() {
        return type;
    }
}
