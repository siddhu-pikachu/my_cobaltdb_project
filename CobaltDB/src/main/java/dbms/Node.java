package dbms;

import java.io.IOException;
import java.io.RandomAccessFile;

public interface Node {
    boolean isLeaf();
    byte getPageType();
    int getPageNumber();
    void setParent(int parentPageNum) throws IOException;
    int getParent() throws IOException;
}

class LeafNode implements Node {
    private final int pageNum;
    private final RandomAccessFile file;
    private Integer nextLeafPageNum;
    private final Page page;
    private final Schema schema;

    public LeafNode(Schema schema, int pageNum, RandomAccessFile file) throws IOException {
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null in LeafNode constructor");
        }
        System.out.println("LeafNode constructor: Received schema: " + schema);
        this.pageNum = pageNum;
        this.file = file;
        this.schema = schema;
        System.out.println("LeafNode constructor: Creating page with schema: " + this.schema);
        this.page = new Page(this.schema, file, pageNum, getPageType());
        this.nextLeafPageNum = null;
        this.page.initialize();
    }

    @Override
    public boolean isLeaf() {
        return true;
    }

    @Override
    public byte getPageType() {
        return 0x0d;
    }

    @Override
    public int getPageNumber() {
        return pageNum;
    }

    @Override
    public void setParent(int parentPageNum) throws IOException {
        page.setParent(parentPageNum);
    }

    @Override
    public int getParent() throws IOException {
        return page.getParent();
    }

    public Page getPage() {
        return page;
    }

    public Integer getNextLeafPageNum() {
        return nextLeafPageNum;
    }

    public void setNextLeafPageNum(Integer nextLeafPageNum) {
        this.nextLeafPageNum = nextLeafPageNum;
    }
}

class InternalNode implements Node {
    private final int pageNum;
    private final RandomAccessFile file;
    private final int[] keys;
    private final Node[] children;
    private int numKeys;
    private int parentPageNum;

    public InternalNode(int pageNum, RandomAccessFile file) {
        this.pageNum = pageNum;
        this.file = file;
        this.keys = new int[4];        // Default order of 4
        this.children = new Node[5];    // keys.length + 1
        this.numKeys = 0;
        this.parentPageNum = -1;
    }

    @Override
    public boolean isLeaf() {
        return false;
    }

    @Override
    public byte getPageType() {
        return 0x05;
    }

    @Override
    public int getPageNumber() {
        return pageNum;
    }

    @Override
    public void setParent(int parentPageNum) throws IOException {
        this.parentPageNum = parentPageNum;
        // Write parent page number to disk
        long offset = (long) pageNum * Page.PAGE_SIZE + 10; // Adjust offset based on header layout
        file.seek(offset);
        file.writeShort(parentPageNum);
    }

    @Override
    public int getParent() throws IOException {
        return parentPageNum;
    }

    public int[] getKeys() {
        return keys;
    }

    public Node[] getChildren() {
        return children;
    }

    public int getNumKeys() {
        return numKeys;
    }

    public void setNumKeys(int numKeys) {
        this.numKeys = numKeys;
    }
}