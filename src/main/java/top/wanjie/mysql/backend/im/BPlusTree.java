package top.wanjie.mysql.backend.im;

import top.wanjie.mysql.backend.common.SubArray;
import top.wanjie.mysql.backend.dm.DataManager;
import top.wanjie.mysql.backend.dm.dataItem.DataItem;
import top.wanjie.mysql.backend.tm.TransactionManagerImpl;
import top.wanjie.mysql.backend.utils.Parser;
import top.wanjie.mysql.backend.im.Node.SearchNextRes;
import top.wanjie.mysql.backend.im.Node.LeafSearchRangeRes;
import top.wanjie.mysql.backend.im.Node.InsertAndSplitRes;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/06/14:57
 */
public class BPlusTree {
    DataManager dm;
    long bootUid;
    // data为rootUid
    DataItem bootDataItem;
    Lock bootLock;

    public static long create(DataManager dm) throws Exception{
        byte[] rawRoot = Node.newNilRootRaw();
        long rootUid = dm.insert(TransactionManagerImpl.SUPER_XID, rawRoot);
        long bootUid = dm.insert(TransactionManagerImpl.SUPER_XID, Parser.long2Byte(rootUid));
        return bootUid;
    }

    public static BPlusTree load(long bootUid, DataManager dm) throws Exception{
        DataItem bootDataItem = dm.read(bootUid);
        assert bootDataItem != null;
        BPlusTree t = new BPlusTree();
        t.dm = dm;
        t.bootDataItem = bootDataItem;
        t.bootUid = bootUid;
        t.bootLock = new ReentrantLock();
        return t;
    }

    private long rootUid() {
        bootLock.lock();
        try {
            SubArray raw = bootDataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(raw.raw, raw.start, raw.start + 8));
        } finally {
            bootLock.unlock();
        }
    }

    private void updateRootUid(long left, long right, long rightKey) throws Exception{
        bootLock.lock();
        try {
            byte[] newRootRaw = Node.newRootRaw(left, right, rightKey);
            long newRootUid = dm.insert(TransactionManagerImpl.SUPER_XID, newRootRaw);
            bootDataItem.before();
            SubArray bdiData = bootDataItem.data();
            System.arraycopy(Parser.long2Byte(newRootUid), 0, bdiData.raw, bdiData.start, 8);
            bootDataItem.after(TransactionManagerImpl.SUPER_XID);
        } finally {
            bootLock.unlock();
        }
    }

    private long searchLeaf(long nodeUid, long key) throws Exception{
        Node node = Node.loadNode(this, nodeUid);
        System.out.println(node);
        boolean isLeaf = node.isLeaf();
        node.release();

        if(isLeaf) {
            return nodeUid;
        } else {
            long next = searchNext(nodeUid, key);
            return searchLeaf(next, key);
        }
    }

    private long searchNext(long nodeUid, long key) throws Exception{
        while (true) {
            Node node = Node.loadNode(this, nodeUid);
            SearchNextRes res = node.searchNext(key);
            node.release();
            if(res.uid != 0) return res.uid;
            else nodeUid = res.siblingUid;
        }
    }

    public List<Long> search(long key) throws Exception{
        return searchRange(key, key);
    }

    public List<Long> searchRange(long leftKey, long rightKey) throws Exception{
        long rootUid = rootUid();
        long leafUid = searchLeaf(rootUid, leftKey);
        ArrayList<Long> uids = new ArrayList<>();
        boolean flag = false;
        while(true) {
            Node leaf = Node.loadNode(this, leafUid);
            if(flag) System.out.println(leaf);
            LeafSearchRangeRes res = leaf.leafSearchRange(leftKey, rightKey);
            leaf.release();
            uids.addAll(res.uids);
            if(res.siblingUid != 0) {
                leafUid = res.siblingUid;
                flag = true;
            }
            else break;
        }
        return uids;
    }

    class InsertRes{
        long newKey;
        long newSon;
    }

    public void insert(long key, long uid) throws Exception{
        long rootUid = rootUid();
        InsertRes insertRes = insert(rootUid, uid, key);
        if(insertRes.newSon != 0) {
            updateRootUid(rootUid, insertRes.newSon, insertRes.newKey);
        }
    }

    private InsertRes insert(long nodeUid, long uid, long key) throws Exception{
        Node node = Node.loadNode(this, nodeUid);
        boolean isLeaf = node.isLeaf();
        node.release();
        InsertRes res = new InsertRes();
        if(isLeaf) {
            return insertAndSplit(nodeUid, uid, key);
        }else {
            long next = searchNext(nodeUid, key);
            InsertRes insertRes = insert(next, uid, key);
            if(insertRes.newSon != 0) {
                res = insertAndSplit(nodeUid, insertRes.newSon, insertRes.newKey);
            }
        }
        return res;
    }

    private InsertRes insertAndSplit(long nodeUid, long uid, long key) throws Exception{
        Node node = Node.loadNode(this, nodeUid);
        InsertAndSplitRes res = node.insertAndSplit(uid, key);
        node.release();
        while(true) {
            if(res.siblingUid != 0) {
                nodeUid = res.siblingUid;
            }else{
                InsertRes insertRes = new InsertRes();
                insertRes.newKey = res.newKey;
                insertRes.newSon = res.newSon;
                return insertRes;
            }
        }
    }

    public void close() {
        bootDataItem.release();
    }

}
