package top.wanjie.mysql.backend.parser;

import top.wanjie.mysql.backend.common.Error;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/07/15:58
 */
public class Tokenizer {
    private byte[] stat;
    private int pos;
    private String currentToken;
    private boolean flushToken;
    private Exception err;

    public Tokenizer(byte[] stat) {
        this.stat = stat;
        this.pos = 0;
        this.currentToken = "";
        this.flushToken = true;
    }

    public String peek() throws Exception{
        if(err != null) {
            throw err;
        }
        if(flushToken) {
            String token = null;
            try{
                token = next();
            } catch (Exception e){
                err = e;
                throw e;
            }
            currentToken = token;
            flushToken = false;
        }
        return currentToken;
    }

    public void pop() {
        this.flushToken = true;
    }

    private String next() throws Exception{
        if(err != null) {
            throw err;
        }
        return nextMetaState();
    }

    private String nextMetaState() throws Exception{
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                return "";
            }else if(!isBlank(b)) {
                break;
            }else{
                popByte();
            }
        }
        Byte b = peekByte();
        if(isSymbol(b)){
            popByte();
            return new String(new byte[]{b});
        } else if(isAlpha(b) || isDigit(b)){
            return nextToken();
        } else if(b == '"' || b == '\'') {
            return nextQuote();
        } else{
            err = Error.InvalidCommandException;
            throw err;
        }
    }

    private String nextToken() throws Exception{
        StringBuilder sb = new StringBuilder();
        while(true){
            Byte b = peekByte();
            if(b == null || !isAlpha(b) && !isDigit(b) && b != '_') {
                if(b != null && isBlank(b)) {
                    popByte();
                }
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }

    private String nextQuote() throws Exception{
        Byte quote = peekByte();
        popByte();
        StringBuilder sb = new StringBuilder();
        while(true) {
            Byte b = peekByte();
            if(b == null) {
                err = Error.InvalidCommandException;
                throw err;
            }
            if(b == quote) {
                popByte();
                break;
            }
            sb.append(new String(new byte[]{b}));
            popByte();
        }
        return sb.toString();
    }


    private void popByte() {
        pos++;
        if(pos > stat.length) {
            pos = stat.length;
        }
    }

    private Byte peekByte() {
        if(pos == stat.length) {
            return null;
        }
        return stat[pos];
    }

    static boolean isDigit(byte b){
        return b >= '0' && b <= '9';
    }

    static boolean isAlpha(byte b) {
        return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z';
    }

    static boolean isSymbol(byte b) {
        return b == '>' || b == '<' || b == '=' || b == '*' || b == ','
                || b == '(' || b == ')';
    }

    static boolean isBlank(byte b) {
        return b == '\t' || b == '\n' || b == ' ';
    }

    public byte[] errStat() {
        byte[] errStat = new byte[stat.length + 3];
        System.arraycopy(stat,0,errStat,0,pos);
        System.arraycopy("<< ".getBytes(),0,errStat,pos,3);
        System.arraycopy(stat,pos,errStat,pos + 3,stat.length - pos);
        return errStat;
    }
}
