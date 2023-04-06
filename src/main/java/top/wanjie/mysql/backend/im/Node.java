package top.wanjie.mysql.backend.im;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/06/14:39
 */

import top.wanjie.mysql.backend.common.SubArray;
import top.wanjie.mysql.backend.dm.dataItem.DataItem;
import top.wanjie.mysql.backend.tm.TransactionManagerImpl;
import top.wanjie.mysql.backend.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Node结构如下：
 * [LeafFlag][KeyNumber][SiblingUid]
 * [Son0][Key0][Son1][Key1]...[SonN][KeyN]
 */
public class Node {
    static final int OF_IS_LEAF = 0;
    static final int NO_KEYS_OFFSET = 1;
    static final int SIBLING_OFFSET = 3;
    static final int NODE_HEADER_SIZE = 11;
    static final int BALANCE_NUMBER = 32;
    static final int NODE_SIZE = NODE_HEADER_SIZE + (2 * 8) * (BALANCE_NUMBER * 2 + 2);

    BPlusTree bPlusTree;
    DataItem dataItem;
    SubArray raw;
    long uid;

    static void setRawIsLeaf(SubArray raw, boolean isLeaf) {
        if(isLeaf) {
            raw.raw[raw.start + OF_IS_LEAF] = (byte)1;
        } else{
            raw.raw[raw.start + OF_IS_LEAF] = (byte)0;
        }
    }

    static boolean getRawIfLeaf(SubArray raw) {
        return raw.raw[raw.start + OF_IS_LEAF] == (byte)1;
    }

    static void setRawNoKeys(SubArray raw, int noKeys) {
        System.arraycopy(Parser.short2Byte((short)noKeys), 0, raw.raw, raw.start+NO_KEYS_OFFSET, 2);
    }

    static int getRawNoKeys(SubArray raw) {
        return (int)Parser.parseShort(Arrays.copyOfRange(raw.raw, raw.start+NO_KEYS_OFFSET, raw.start+NO_KEYS_OFFSET+2));
    }

    static void setRawSibling(SubArray raw, long sibling) {
        System.arraycopy(Parser.long2Byte(sibling), 0, raw.raw, raw.start+SIBLING_OFFSET, 8);
    }

    static long getRawSibling(SubArray raw) {
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start+SIBLING_OFFSET, raw.start+SIBLING_OFFSET+8));
    }

    static void setRawKthSon(SubArray raw, long uid, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(Parser.long2Byte(uid), 0, raw.raw, offset, 8);
    }

    static long getRawKthSon(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2);
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    static void setRawKthKey(SubArray raw, long key, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        System.arraycopy(Parser.long2Byte(key), 0, raw.raw, offset, 8);
    }

    static long getRawKthKey(SubArray raw, int kth) {
        int offset = raw.start+NODE_HEADER_SIZE+kth*(8*2)+8;
        return Parser.parseLong(Arrays.copyOfRange(raw.raw, offset, offset+8));
    }

    static void copyRawFromKth(SubArray from, SubArray to, int kth) {
        // 将节点from中从第k个子节点拷贝到节点to的开头
        int offset = from.start+NODE_HEADER_SIZE+kth*(8*2);
        System.arraycopy(from.raw, offset, to.raw, to.start+NODE_HEADER_SIZE, from.end-offset);
    }

    static void shiftRawKth(SubArray raw,int kth) {
        int begin = raw.start + NODE_HEADER_SIZE + (kth + 1) * 2 * 8;
        int end = raw.start + NODE_SIZE - 1;
        for(int i = end;i >= begin;i--) {
            raw.raw[i] = raw.raw[i - 2 * 8];
        }
    }

    static byte[] newRootRaw(long left, long right, long rightKey) {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw,false);
        setRawNoKeys(raw,2);
        setRawSibling(raw, 0);
        setRawKthSon(raw, left, 0);
        setRawKthKey(raw, rightKey, 0);
        setRawKthSon(raw, right, 1);
        setRawKthKey(raw, Long.MAX_VALUE, 1);

        return raw.raw;
    }

    static byte[] newNilRootRaw() {
        SubArray raw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawIsLeaf(raw,true);
        setRawNoKeys(raw,0);
        setRawSibling(raw, 0);

        return raw.raw;
    }

    static Node loadNode(BPlusTree tree, long uid) throws Exception{
        DataItem di = tree.dm.read(uid);
        assert di != null;
        Node n = new Node();
        n.bPlusTree = tree;
        n.dataItem = di;
        n.raw = di.data();
        n.uid = uid;
        return n;
    }

    public void release() {
        dataItem.release();
    }

    public boolean isLeaf() {
        dataItem.rLock();
        try {
            return getRawIfLeaf(raw);
        } finally {
            dataItem.rUnLock();
        }
    }

    class SearchNextRes{
        long uid;
        long siblingUid;
    }

    public SearchNextRes searchNext(long key) {
        dataItem.rLock();
        try{
            int noKeys = getRawNoKeys(raw);
            SearchNextRes res = new SearchNextRes();
            for (int i = 0; i < noKeys; i++) {
                long ik = getRawKthKey(raw, i);
                if(key < ik) {
                    res.uid = getRawKthSon(raw, i);
                    res.siblingUid = 0;
                    return res;
                }
            }
            res.uid = 0;
            res.siblingUid = getRawSibling(raw);
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class LeafSearchRangeRes{
        List<Long> uids = new ArrayList<>();
        long siblingUid = 0;
    }

    // 叶子节点范围查询
    public LeafSearchRangeRes leafSearchRange(long leftKey, long rightKey) {
        dataItem.rLock();
        int kth = 0;
        try {
            int noKeys = getRawNoKeys(raw);
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik >= leftKey) break;
                kth++;
            }
            LeafSearchRangeRes res = new LeafSearchRangeRes();
            while(kth < noKeys) {
                long ik = getRawKthKey(raw, kth);
                if(ik <= rightKey) {
                    res.uids.add(getRawKthSon(raw, kth));
                    kth++;
                }else {
                    break;
                }
            }

            if(kth == noKeys) {
                res.siblingUid = getRawSibling(raw);
            }
            return res;
        } finally {
            dataItem.rUnLock();
        }
    }

    class InsertAndSplitRes {
        long siblingUid;
        long newKey;
        long newSon;
    }

    public InsertAndSplitRes insertAndSplit(long uid, long key) throws Exception{
        boolean success = false;
        Exception err = null;
        InsertAndSplitRes res = new InsertAndSplitRes();
        dataItem.before();
        try {
            success = insert(uid, key);
            if(!success) {
                res.siblingUid = getRawSibling(raw);
                return res;
            }
            if(needSplit()) {
                try {
                    SplitRes splitRes = split();
                    res.newKey = splitRes.newKey;
                    res.newSon = splitRes.newSon;
                    return res;
                } catch (Exception e){
                    err = e;
                    throw e;
                }

            }
            return res;
        } finally {
            if(success && err == null) {
                dataItem.after(TransactionManagerImpl.SUPER_XID);
            } else {
                dataItem.unBefore();
            }
        }
    }

    private boolean insert(long uid,long key) {
        int noKeys = getRawNoKeys(raw);
        int kth = 0;
        while(kth < noKeys) {
            long ik = getRawKthKey(raw, kth);
            if(ik < key){
                kth++;
            }else {
                break;
            }
        }

        if(kth == noKeys && getRawSibling(raw) != 0) return false;
        if(getRawIfLeaf(raw)) {
            shiftRawKth(raw,kth);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth);
            setRawNoKeys(raw, noKeys + 1);
        } else {
            shiftRawKth(raw, kth + 1);
            long kk = getRawKthKey(raw, kth);
            setRawKthKey(raw, kk, kth + 1);
            setRawKthKey(raw, key, kth);
            setRawKthSon(raw, uid, kth + 1);
            setRawNoKeys(raw ,noKeys + 1);
        }
        return true;
    }

    private boolean needSplit() {
        return BALANCE_NUMBER*2 == getRawNoKeys(raw);
    }

    class SplitRes {
        long newSon, newKey;
    }

    private SplitRes split() throws Exception {
        SubArray newraw = new SubArray(new byte[NODE_SIZE], 0, NODE_SIZE);
        setRawNoKeys(newraw, BALANCE_NUMBER);
        setRawIsLeaf(newraw, getRawIfLeaf(raw));
        setRawSibling(newraw, getRawSibling(raw));
        copyRawFromKth(raw, newraw, BALANCE_NUMBER);

        long son = bPlusTree.dm.insert(TransactionManagerImpl.SUPER_XID, newraw.raw);
        setRawNoKeys(raw, BALANCE_NUMBER);
        setRawSibling(raw, son);

        SplitRes res = new SplitRes();
        res.newKey = getRawKthKey(newraw , 0);
        res.newSon = son;
        return res;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Is leaf: ").append(getRawIfLeaf(raw)).append("\n");
        int KeyNumber = getRawNoKeys(raw);
        sb.append("KeyNumber: ").append(KeyNumber).append("\n");
        sb.append("sibling: ").append(getRawSibling(raw)).append("\n");
        for(int i = 0; i < KeyNumber; i ++) {
            sb.append("son: ").append(getRawKthSon(raw, i)).append(", key: ").append(getRawKthKey(raw, i)).append("\n");
        }
        return sb.toString();
    }

    }
