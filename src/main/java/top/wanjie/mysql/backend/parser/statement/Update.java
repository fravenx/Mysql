package top.wanjie.mysql.backend.parser.statement;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/07/17:17
 */
public class Update {
    public String tableName;
    public String fieldName;
    public String value;
    public Where where;
}
