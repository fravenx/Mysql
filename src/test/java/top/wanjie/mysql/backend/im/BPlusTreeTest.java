package top.wanjie.mysql.backend.im;

import org.junit.Test;
import top.wanjie.mysql.backend.dm.DataManager;
import top.wanjie.mysql.backend.dm.pageCache.PageCache;
import top.wanjie.mysql.backend.tm.MockTransactionManager;
import top.wanjie.mysql.backend.tm.TransactionManager;

import java.io.File;
import java.util.List;

public class BPlusTreeTest {
    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create("/tmp/TestTreeSingle5", PageCache.PAGE_SIZE*10, tm);

        long root = BPlusTree.create(dm);
        BPlusTree tree = BPlusTree.load(root, dm);

        int lim = 100;
        for(int i = 1; i < lim; i ++) {
            if(i == 64) {
                System.out.println();
            }
            tree.insert(i, i);
        }

        for(int i = 1; i < lim; i ++) {
            List<Long> uids = tree.search(i);
            System.out.println(i);
            System.out.println(uids.size());
            assert uids.size() == 1;
            assert uids.get(0) == i;
//            System.out.println("uid = " + i + "\n");
//            Node node = Node.loadNode(tree,(long)i);
//            System.out.println(node);
        }

        assert new File("/tmp/TestTreeSingle5.db").delete();
        assert new File("/tmp/TestTreeSingle5.log").delete();
    }
}
