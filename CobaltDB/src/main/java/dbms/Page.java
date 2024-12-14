package dbms;

import java.io.RandomAccessFile;
import java.io.IOException;
import java.util.Arrays;

public class Page {
    public static final int PAGE_SIZE = 512;
    private static final int HEADER_SIZE = 16;
    private static final int OFFSET_SIZE = 2;
    private final Schema schema;
    private final int RECORD_SIZE;  // Changed to final and initialized in constructor
    private final int MAX_RECORDS;   // Changed to final and initialized in constructor

    private final RandomAccessFile file;
    private final int pageNumber;
    private final byte pageType;
    private short recordCount;
    private short cellContentStart;
    private short rootPage;
    private short rightSibling;
    private short parentPage;
    private final short[] cellOffsets;

    public Page(Schema schema, RandomAccessFile file, int pageNumber, byte pageType) {
        if (schema == null) {
            throw new IllegalArgumentException("Schema cannot be null in Page constructor");
        }
        System.out.println("Page constructor: Received schema: " + schema);
        this.schema = schema;
        this.file = file;
        this.pageNumber = pageNumber;
        this.pageType = pageType;

        // Initialize RECORD_SIZE and MAX_RECORDS
        this.RECORD_SIZE = calculateRecordSize();
        this.MAX_RECORDS = (PAGE_SIZE - HEADER_SIZE) / (this.RECORD_SIZE + OFFSET_SIZE);

        this.cellOffsets = new short[MAX_RECORDS];
        this.rightSibling = -1;
        this.parentPage = -1;
    }

    private int calculateRecordSize() {
        if (schema == null) {
            throw new IllegalStateException("Schema is null when calculating record size");
        }
        int fieldSizes = schema.getFields().stream()
                .mapToInt(field -> {
                    switch (field.getType()) {
                        case "string": return field.getLength();
                        case "int": return 4;
                        case "short": return 2;
                        default: throw new IllegalArgumentException("Unsupported type: " + field.getType());
                    }
                }).sum();

        return 4 + fieldSizes + 1; // Row ID (4 bytes) + Field Data + Deletion Marker (1 byte)
    }

    public void initialize() throws IOException {
        file.seek(pageNumber * PAGE_SIZE);

        // Write header (16 bytes)
        file.writeByte(pageType);        // Page type (1 byte)
        file.writeByte(0);               // Unused (1 byte)
        file.writeShort(0);              // Number of cells (2 bytes)
        file.writeShort(PAGE_SIZE);      // Start of cell content (2 bytes)
        file.writeShort(rootPage);       // Root page number (2 bytes)
        file.writeShort(rightSibling);   // Right sibling/child (2 bytes)
        file.writeShort(parentPage);     // Parent page (2 bytes)
        file.writeInt(0);                // Unused (4 bytes)

        recordCount = 0;
        cellContentStart = (short) PAGE_SIZE;
    }

    public boolean hasSpace(int recordSize) {
        int neededSpace = recordSize + 2;  // record + offset entry
        int availableSpace = cellContentStart - (HEADER_SIZE + recordCount * 2);
        return availableSpace >= neededSpace;
    }

    public boolean addRecord(Record record) throws IOException {
        System.out.println("Serialized record: " + Arrays.toString(record.serialize()));
        byte[] recordData = record.serialize();
        if (!hasSpace(recordData.length + 6)) { // +6 for payload size(2) + rowId(4)
            return false;
        }

        cellContentStart = (short) (cellContentStart - recordData.length - 6);
        file.seek(pageNumber * PAGE_SIZE + cellContentStart);
        file.writeByte(recordData.length);    // Write payload size
        file.write(recordData);               // Write actual record data

        // Add cell offset to array (maintained in sorted order by rowId)
        int insertPos = 0;
        while (insertPos < recordCount && getCellRowId(cellOffsets[insertPos]) < record.getRowId()) {
            insertPos++;
        }

        // Shift existing offsets
        System.arraycopy(cellOffsets, insertPos,
                cellOffsets, insertPos + 1,
                recordCount - insertPos);

        // Insert new offset
        cellOffsets[insertPos] = cellContentStart;

        recordCount++;
        updateHeader();
        return true;
    }

    private int getCellRowId(short offset) throws IOException {
        file.seek(pageNumber * PAGE_SIZE + offset);
        if (pageType == 0x0d) {  // Table Leaf
            file.skipBytes(1);    // Skip payload size
            return file.readInt();     // Read rowId
        } else if (pageType == 0x05) {  // Table Interior
            file.skipBytes(2);    // Skip left child pointer
            return file.readInt();     // Read rowId
        } else {
            throw new IllegalStateException("Invalid page type: " + pageType);
        }
    }

    private boolean updateHeader() throws IOException {
        file.seek(pageNumber * PAGE_SIZE);

        file.writeByte(pageType);
        file.writeByte(0);
        file.writeShort(recordCount);
        file.writeShort(cellContentStart);
        file.writeShort(rootPage);
        file.writeShort(rightSibling);
        file.writeShort(parentPage);
        file.writeInt(0);

        // Write cell offsets array
        long offsetPos = pageNumber * PAGE_SIZE + HEADER_SIZE;
        for (int i = 0; i < recordCount; i++) {
            file.seek(offsetPos);
            file.writeShort(cellOffsets[i]);
            offsetPos += 2;
        }
        return true;
    }

    public Record[] getAllRecords() throws IOException {
        Record[] records = new Record[recordCount];

        // First read cell offsets from header area
        file.seek(pageNumber * PAGE_SIZE + HEADER_SIZE);
        short[] offsets = new short[recordCount];
        for (int i = 0; i < recordCount; i++) {
            offsets[i] = file.readShort();
        }

        // Now read records using these offsets
        for (int i = 0; i < recordCount; i++) {
            file.seek(pageNumber * PAGE_SIZE + offsets[i]);

            byte payloadSize = file.readByte();
            int rowId = file.readInt();

            byte[] recordData = new byte[payloadSize];
            file.read(recordData);

            records[i] = Record.deserialize(schema, recordData, rowId);
        }

        return records;
    }

    public void clear() throws IOException {
        file.seek(pageNumber * PAGE_SIZE);

        // Reinitialize header
        file.writeByte(pageType);
        file.writeByte(0);
        file.writeShort(0);                // record count
        file.writeShort(PAGE_SIZE);        // cell content start
        file.writeShort(rootPage);
        file.writeShort(rightSibling);
        file.writeShort(parentPage);
        file.writeInt(0);

        // Reset page state in memory
        recordCount = 0;
        cellContentStart = (short) PAGE_SIZE;
        for (int i = 0; i < cellOffsets.length; i++) {
            cellOffsets[i] = 0;
        }

        // Explicitly clear the cell offset array area in file
        file.seek(pageNumber * PAGE_SIZE + HEADER_SIZE);
        for (int i = 0; i < MAX_RECORDS; i++) {
            file.writeShort(0);
        }

        // Clear the content area
        file.seek(pageNumber * PAGE_SIZE + HEADER_SIZE + (MAX_RECORDS * 2));
        byte[] emptyContent = new byte[PAGE_SIZE - HEADER_SIZE - (MAX_RECORDS * 2)];
        file.write(emptyContent);
    }

    public int getParent() {
        return parentPage;
    }

    public void setParent(int parent) throws IOException {
        this.parentPage = (short) parent;
        updateHeader();
    }

    public void setRightSibling(int sibling) throws IOException {
        System.out.println("Setting right sibling of page " + pageNumber + " to " + sibling);
        this.rightSibling = (short) sibling;
        updateHeader();
    }

    public Integer getRightSibling() {
        return rightSibling == -1 ? null : (int) rightSibling;
    }

    public void setRightChild(int child) throws IOException {
        System.out.println("Setting right child of interior page " + pageNumber + " to " + child);
        this.rightSibling = (short) child;  // Uses same header location as rightSibling
        updateHeader();
    }

    public void writeInteriorCell(int leftChild, int key) throws IOException {
        // Calculate cell content start position
        cellContentStart = (short)(cellContentStart - 6);  // 2 bytes for child + 4 bytes for key

        // Add cell offset to header array
        cellOffsets[recordCount] = cellContentStart;
        recordCount++;

        // Write cell content
        file.seek(pageNumber * PAGE_SIZE + cellContentStart);
        file.writeShort(leftChild);  // Left child pointer
        file.writeInt(key);          // Key value

        // Update header
        updateHeader();
    }
}