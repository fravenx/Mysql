package top.wanjie.mysql.backend.dm;

import org.checkerframework.checker.units.qual.A;
import top.wanjie.mysql.backend.common.AbstractCache;
import top.wanjie.mysql.backend.common.Error;
import top.wanjie.mysql.backend.dm.dataItem.DataItem;
import top.wanjie.mysql.backend.dm.dataItem.DataItemImpl;
import top.wanjie.mysql.backend.dm.logger.Logger;
import top.wanjie.mysql.backend.dm.page.Page;
import top.wanjie.mysql.backend.dm.page.PageOne;
import top.wanjie.mysql.backend.dm.page.PageX;
import top.wanjie.mysql.backend.dm.pageCache.PageCache;
import top.wanjie.mysql.backend.dm.pageIndex.PageIndex;
import top.wanjie.mysql.backend.dm.pageIndex.PageInfo;
import top.wanjie.mysql.backend.tm.TransactionManager;
import top.wanjie.mysql.backend.utils.Types;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/04/12:34
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{
    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }


    void initPageOne() {
        int pgno = pc.newPage(PageOne.initRaw());
        assert pgno == 1;
        pageOne = pc.getPage(pgno);
    }

    boolean loadCheckPageOne() {
        pageOne = pc.getPage(1);
        return PageOne.checkVc(pageOne);
    }

    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2;i <= pageNumber;i++) {
            Page page = pc.getPage(i);
            pIndex.add(i, PageX.getFreeSpage(page));
            page.release();
        }
    }

    // 修改数据时下日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.wrapUpdateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }


    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    // 1 封装dataItem
    // 2 选出一个大小合适的页面
    // 3 取出该页，下日志，插入数据，将页面放回pIndex中，返回uid
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        for(int i = 0;i < 5;i++) {
            pi = pIndex.select(raw.length);
            if(pi != null) {
                break;
            }else {
                int pgno = pc.newPage(PageX.initRaw());
                pIndex.add(pgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }
        Page pg = pc.getPage(pi.pgno);
        byte[] log = Recover.wrapInsertLog(xid, pg, raw);
        logger.log(log);
        short offset = PageX.insert(pg, raw);
        pg.release();
        pIndex.add(pg.getPageNumber(), PageX.getFreeSpage(pg));
        return Types.addressToUid(pg.getPageNumber(), offset);
    }

    // 关闭DataItem缓存，日志，设置第一页字节校验，关闭页面缓存
    @Override
    public void close() {
        super.close();
        logger.close();
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }
}
