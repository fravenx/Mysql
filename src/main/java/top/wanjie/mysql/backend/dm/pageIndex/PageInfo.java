package top.wanjie.mysql.backend.dm.pageIndex;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/03/16:43
 */
public class PageInfo {
    public int pgno;
    public int freeSpace;

    public PageInfo(int pgno, int freeSpace) {
        this.pgno = pgno;
        this.freeSpace = freeSpace;
    }
}
