package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.common.Buffer;
import edu.berkeley.cs186.database.common.ByteBuf;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.table.RecordId;

import java.util.*;

/**
 * A leaf of a B+ tree. Every leaf in a B+ tree of order d stores between d and
 * 2d (key, record id) pairs and a pointer to its right sibling (i.e. the page
 * number of its right sibling). Moreover, every leaf node is serialized and
 * persisted on a single page; see toBytes and fromBytes for details on how a
 * leaf is serialized. For example, here is an illustration of two order 2
 * leafs connected together:
 * <p>
 * leaf 1 (stored on some page)          leaf 2 (stored on some other page)
 * +-------+-------+-------+-------+     +-------+-------+-------+-------+
 * | k0:r0 | k1:r1 | k2:r2 |       | --> | k3:r3 | k4:r4 |       |       |
 * +-------+-------+-------+-------+     +-------+-------+-------+-------+
 */
class LeafNode extends BPlusNode {
    /**
     * Metadata about the B+ tree that this node belongs to.
     */
    private final BPlusTreeMetadata metadata;

    // Buffer manager
    private final BufferManager bufferManager;

    // Lock context of the B+ tree
    private final LockContext treeContext;

    // The page on which this leaf is serialized.
    private final Page page;

    /**
     * The keys and record ids of this leaf. `keys` is always sorted in ascending
     * order. The record id at index i corresponds to the key at index i. For
     * example, the keys [a, b, c] and the rids [1, 2, 3] represent the pairing
     * [a:1, b:2, c:3].
     * Note the following subtlety. keys and rids are in-memory caches of the
     * keys and record ids stored on disk. Thus, consider what happens when you
     * create two LeafNode objects that point to the same page:
     * BPlusTreeMetadata meta = ...;
     * int pageNum = ...;
     * LockContext treeContext = new DummyLockContext();
     * LeafNode leaf0 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
     * LeafNode leaf1 = LeafNode.fromBytes(meta, bufferManager, treeContext, pageNum);
     * This scenario looks like this:
     * HEAP                        | DISK
     * ===============================================================
     * leaf0                       | page 42
     * +-------------------------+ | +-------+-------+-------+-------+
     * | keys = [k0, k1, k2]     | | | k0:r0 | k1:r1 | k2:r2 |       |
     * | rids = [r0, r1, r2]     | | +-------+-------+-------+-------+
     * | pageNum = 42            | |
     * +-------------------------+ |
     * |
     * leaf1                       |
     * +-------------------------+ |
     * | keys = [k0, k1, k2]     | |
     * | rids = [r0, r1, r2]     | |
     * | pageNum = 42            | |
     * +-------------------------+ |
     * |
     * Now imagine we perform on operation on leaf0 like leaf0.put(k3, r3). The
     * in-memory values of leaf0 will be updated and they will be synced to disk.
     * But, the in-memory values of leaf1 will not be updated. That will look
     * like this:
     * HEAP                        | DISK
     * ===============================================================
     * leaf0                       | page 42
     * +-------------------------+ | +-------+-------+-------+-------+
     * | keys = [k0, k1, k2, k3] | | | k0:r0 | k1:r1 | k2:r2 | k3:r3 |
     * | rids = [r0, r1, r2, r3] | | +-------+-------+-------+-------+
     * | pageNum = 42            | |
     * +-------------------------+ |
     * |
     * leaf1                       |
     * +-------------------------+ |
     * | keys = [k0, k1, k2]     | |
     * | rids = [r0, r1, r2]     | |
     * | pageNum = 42            | |
     * +-------------------------+ |
     * |
     * Make sure your code (or your tests) doesn't use stale in-memory cached
     * values of keys and rids.
     */
    private List<DataBox> keys;
    private List<RecordId> rids;

    // If this leaf is the rightmost leaf, then rightSibling is Optional.empty().
    // Otherwise, rightSibling is Optional.of(n) where n is the page number of
    // this leaf's right sibling.
    private Optional<Long> rightSibling;

    // Constructors ////////////////////////////////////////////////////////////

    /**
     * Construct a brand new leaf node. This constructor will fetch a new pinned
     * page from the provided BufferManager `bufferManager` and persist the node
     * to that page.
     */
    LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, List<DataBox> keys,
             List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        this(metadata, bufferManager, bufferManager.fetchNewPage(treeContext, metadata.getPartNum()),
                keys, rids,
                rightSibling, treeContext);
    }

    /**
     * Construct a leaf node that is persisted to page `page`.
     */
    private LeafNode(BPlusTreeMetadata metadata, BufferManager bufferManager, Page page,
                     List<DataBox> keys,
                     List<RecordId> rids, Optional<Long> rightSibling, LockContext treeContext) {
        try {
            assert (keys.size() == rids.size());
            assert (keys.size() <= 2 * metadata.getOrder());

            this.metadata = metadata;
            this.bufferManager = bufferManager;
            this.treeContext = treeContext;
            this.page = page;
            this.keys = new ArrayList<>(keys);
            this.rids = new ArrayList<>(rids);
            this.rightSibling = rightSibling;

            sync();
        } finally {
            page.unpin();
        }
    }

    /**
     * Returns the largest number d such that the serialization of a LeafNode
     * with 2d entries will fit on a single page.
     */
    static int maxOrder(short pageSize, Type keySchema) {
        // A leaf node with n entries takes up the following number of bytes:
        //
        //   1 + 8 + 4 + n * (keySize + ridSize)
        //
        // where
        //
        //   - 1 is the number of bytes used to store isLeaf,
        //   - 8 is the number of bytes used to store a sibling pointer,
        //   - 4 is the number of bytes used to store n,
        //   - keySize is the number of bytes used to store a DataBox of type
        //     keySchema, and
        //   - ridSize is the number of bytes of a RecordId.
        //
        // Solving the following equation
        //
        //   n * (keySize + ridSize) + 13 <= pageSizeInBytes
        //
        // we get
        //
        //   n = (pageSizeInBytes - 13) / (keySize + ridSize)
        //
        // The order d is half of n.
        int keySize = keySchema.getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int n = (pageSize - 13) / (keySize + ridSize);
        return n / 2;
    }

    /**
     * Loads a leaf node from page `pageNum`.
     */
    public static LeafNode fromBytes(BPlusTreeMetadata metadata, BufferManager bufferManager,
                                     LockContext treeContext, long pageNum) {
        // TODO(proj2): implement
        // Note: LeafNode has two constructors. To implement fromBytes be sure to
        // use the constructor that reuses an existing page instead of fetching a
        // brand new one.
        final Page page = bufferManager.fetchPage(treeContext, pageNum);
        final Buffer buffer = page.getBuffer();
        final byte nodeType = buffer.get();

        assert nodeType == NodeType.LEAF.getType();

        final long rightSiblingPageId = buffer.getLong();
        final int pairNums = buffer.getInt();
        final List<DataBox> keys = new ArrayList<>(pairNums);
        final List<RecordId> recordIds = new ArrayList<>(pairNums);
        for (int i = 0; i < pairNums; i++) {
            //从buffer反序列化出KeySchema（对应类型）的DataBox
            keys.add(DataBox.fromBytes(buffer, metadata.getKeySchema()));
            final long leafPageNum = buffer.getLong();
            final short leafEntryNum = buffer.getShort();
            final RecordId recordId = new RecordId(leafPageNum, leafEntryNum);
            recordIds.add(recordId);
        }
        return new LeafNode(metadata, bufferManager, page, keys, recordIds, Optional.of(rightSiblingPageId), treeContext);
    }

    /**
     * Core API
     *
     * @param key key
     * @return leafNode corresponding to the key
     * @see BPlusNode#get(DataBox)
     */
    @Override
    public LeafNode get(DataBox key) {
        // TODO(proj2): implement
        return this;
    }

    /**
     * @return LeftmostLeaf
     * @see BPlusNode#getLeftmostLeaf()
     */
    @Override
    public LeafNode getLeftmostLeaf() {
        // TODO(proj2): implement
        return this;
    }

    /**
     * @param key key
     * @param rid recordId
     * @return pair of split node info if it needs to be split
     * @see BPlusNode#put(DataBox, RecordId)
     */
    @Override
    public Optional<Pair<DataBox, Long>> put(DataBox key, RecordId rid) {
        // TODO(proj2): implement
        if (keys.contains(key)) {
            throw new BPlusTreeException(String.format("Duplicated key %s is inserted.", key.toString()));
        }
        final int index = InnerNode.numLessThan(key, keys);

        //get d of the B+ tree
        final int order = metadata.getOrder();
        //rids and keys are aligned
        keys.add(index, key);
        rids.add(index, rid);
        //key size should be less than 2 * order(2*d)
        final int keySizeLimit = 2 * order;
        final int keysSize = keys.size();
        if (keysSize <= keySizeLimit) {
            //non overflow
            sync();
            return Optional.empty();
        }
        if (keysSize > keySizeLimit + 1) {
            throw new BPlusTreeException("Overflow when size is larger than 2d+1");
        }
        return doSplit(order);
    }

    @Override
    protected Optional<Pair<DataBox, Long>> doSplit(int splitIndex) {
        final int currentSize = keys.size();
        final List<DataBox> leftKeys = keys.subList(0, splitIndex);
        final List<DataBox> rightKeys = keys.subList(splitIndex, currentSize);
        final List<RecordId> leftRids = rids.subList(0, splitIndex);
        final List<RecordId> rightRids = rids.subList(splitIndex, currentSize);

        final LeafNode rightNode = new LeafNode(
                this.metadata,
                this.bufferManager,
                rightKeys,
                rightRids,
                this.rightSibling,
                treeContext);

        final long rightNodePageNum = rightNode.getPage().getPageNum();
        //update
        keys = leftKeys;
        rids = leftRids;
        rightSibling = Optional.of(rightNodePageNum);
        sync();
        //push up
        return Optional.of(new Pair<>(rightKeys.get(0), rightNodePageNum));
    }

    // Iterators ///////////////////////////////////////////////////////////////

    /**
     * @see BPlusNode#bulkLoad(Iterator, float)
     */
    @Override
    public Optional<Pair<DataBox, Long>> bulkLoad(Iterator<Pair<DataBox, RecordId>> data,
                                                  float fillFactor) {
        // TODO(proj2): implement
        if (fillFactor <= 0) {
            throw new IllegalArgumentException("FillFactor should be in (0,1].");
        }
        //do populate
        //上取整
        final int order = metadata.getOrder();
        final int limit = (int) Math.ceil(2 * order * fillFactor);
        for (int i = keys.size(); i < limit && data.hasNext(); i++) {
            final Pair<DataBox, RecordId> pair = data.next();
            keys.add(pair.getFirst());
            rids.add(pair.getSecond());
        }

        if (!data.hasNext()) {
            //perfectly filled
            sync();
            return Optional.empty();
        }

        //split
        final Pair<DataBox, RecordId> rightSiblingInfo = data.next();
        final DataBox key = rightSiblingInfo.getFirst();
        final RecordId recordId = rightSiblingInfo.getSecond();

        final List<DataBox> rightSiblingKeys = new ArrayList<>();
        final List<RecordId> rightSiblingRids = new ArrayList<>();
        //just put one pair of data into the right sibling,if iterator has next,keep them for inner node to process
        rightSiblingKeys.add(key);
        rightSiblingRids.add(recordId);
        final LeafNode rightLeafNode = new LeafNode(
                this.metadata,
                this.bufferManager,
                rightSiblingKeys,
                rightSiblingRids,
                this.rightSibling,
                this.treeContext
        );
        sync();
        return Optional.of(new Pair<>(key, rightLeafNode.getPage().getPageNum()));
    }

    /**
     * @param key key
     * @see BPlusNode#remove(DataBox)
     */
    @Override
    public void remove(DataBox key) {
        // TODO(proj2): implement
        if (keys.contains(key)) {
            rids.remove(keys.indexOf(key));
            keys.remove(key);
            sync();
        }
    }

    /**
     * Return the record id associated with `key`.
     */
    Optional<RecordId> getKey(DataBox key) {
        int index = keys.indexOf(key);
        return index == -1 ? Optional.empty() : Optional.of(rids.get(index));
    }

    /**
     * Returns an iterator over the record ids of this leaf in ascending order of
     * their corresponding keys.
     */
    Iterator<RecordId> scanAll() {
        return rids.iterator();
    }

    /**
     * Returns an iterator over the record ids of this leaf that have a
     * corresponding key greater than or equal to `key`. The record ids are
     * returned in ascending order of their corresponding keys.
     */
    Iterator<RecordId> scanGreaterEqual(DataBox key) {
        int index = InnerNode.numLessThan(key, keys);
        return rids.subList(index, rids.size()).iterator();
    }

    // Helpers /////////////////////////////////////////////////////////////////
    @Override
    public Page getPage() {
        return page;
    }

    /**
     * Returns the right sibling of this leaf, if it has one.
     */
    Optional<LeafNode> getRightSibling() {
        if (!rightSibling.isPresent()) {
            return Optional.empty();
        }

        long pageNum = rightSibling.get();
        if (pageNum < 0) {
            /*TODO(proj2):
             *  If the right sibling does not exists,the page num will be -1,we'd better do prejudgment here.
             *  This part is not in original proj2 scope.
             */
            return Optional.empty();
        }
        return Optional.of(LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum));
    }

    /**
     * Serializes this leaf to its page.
     */
    private void sync() {
        page.pin();
        try {
            Buffer b = page.getBuffer();
            byte[] newBytes = toBytes();
            byte[] bytes = new byte[newBytes.length];
            b.get(bytes);
            if (!Arrays.equals(bytes, newBytes)) {
                page.getBuffer().put(toBytes());
            }
        } finally {
            page.unpin();
        }
    }

    // Just for testing.
    List<DataBox> getKeys() {
        return keys;
    }

    // Just for testing.
    List<RecordId> getRids() {
        return rids;
    }

    // Pretty Printing /////////////////////////////////////////////////////////
    @Override
    public String toString() {
        String rightSibString = rightSibling.map(Object::toString).orElse("None");
        return String.format("LeafNode(pageNum=%s, keys=%s, rids=%s, rightSibling=%s)",
                page.getPageNum(), keys, rids, rightSibString);
    }

    @Override
    public String toSexp() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            String key = keys.get(i).toString();
            String rid = rids.get(i).toSexp();
            ss.add(String.format("(%s %s)", key, rid));
        }
        return String.format("(%s)", String.join(" ", ss));
    }

    /**
     * Given a leaf with page number 1 and three (key, rid) pairs (0, (0, 0)),
     * (1, (1, 1)), and (2, (2, 2)), the corresponding dot fragment is:
     * <p>
     * node1[label = "{0: (0 0)|1: (1 1)|2: (2 2)}"];
     */
    @Override
    public String toDot() {
        List<String> ss = new ArrayList<>();
        for (int i = 0; i < keys.size(); ++i) {
            ss.add(String.format("%s: %s", keys.get(i), rids.get(i).toSexp()));
        }
        long pageNum = getPage().getPageNum();
        String s = String.join("|", ss);
        return String.format("  node%d[label = \"{%s}\"];", pageNum, s);
    }

    // Serialization ///////////////////////////////////////////////////////////
    @Override
    public byte[] toBytes() {
        // When we serialize a leaf node, we write:
        //
        //   a. the literal value 1 (1 byte) which indicates that this node is a
        //      leaf node,
        //   b. the page id (8 bytes) of our right sibling (or -1 if we don't have
        //      a right sibling),
        //   c. the number (4 bytes) of (key, rid) pairs this leaf node contains,
        //      and
        //   d. the (key, rid) pairs themselves.
        //
        // For example, the following bytes:
        //
        //   +----+-------------------------+-------------+----+-------------------------------+
        //   | 01 | 00 00 00 00 00 00 00 04 | 00 00 00 01 | 03 | 00 00 00 00 00 00 00 03 00 01 |
        //   +----+-------------------------+-------------+----+-------------------------------+
        //    \__/ \_______________________/ \___________/ \__________________________________/
        //     a               b                   c                         d
        //
        // represent a leaf node with sibling on page 4 and a single (key, rid)
        // pair with key 3 and page id (3, 1).

        assert (keys.size() == rids.size());
        assert (keys.size() <= 2 * metadata.getOrder());

        // All sizes are in bytes.

        //keep 1 byte for node type
        int isLeafSize = 1;
        int siblingSize = Long.BYTES;
        int lenSize = Integer.BYTES;
        int keySize = metadata.getKeySchema().getSizeInBytes();
        int ridSize = RecordId.getSizeInBytes();
        int entriesSize = (keySize + ridSize) * keys.size();
        int size = isLeafSize + siblingSize + lenSize + entriesSize;
        final Buffer buffer = ByteBuf.allocate(size);
        //TODO:Try to use ByteBuf
        buffer.put((byte) 1);
        buffer.putLong(rightSibling.orElse(-1L));
        buffer.putInt(keys.size());
        for (int i = 0; i < keys.size(); ++i) {
            buffer.put(keys.get(i).toBytes());
            buffer.put(rids.get(i).toBytes());
        }
        return ((ByteBuf) buffer).array();
    }

    // Builtins ////////////////////////////////////////////////////////////////
    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof LeafNode)) {
            return false;
        }
        LeafNode n = (LeafNode) o;
        return page.getPageNum() == n.page.getPageNum() &&
                keys.equals(n.keys) &&
                rids.equals(n.rids) &&
                rightSibling.equals(n.rightSibling);
    }

    @Override
    public int hashCode() {
        return Objects.hash(page.getPageNum(), keys, rids, rightSibling);
    }
}
