package dbms;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;

public class FileStorage {
    private static final int PAGE_SIZE = 512;
    private final Map<Table, RandomAccessFile> file = new HashMap<>();
    private final Map<String, Table> table = new HashMap<>();
    private final String filename;
    private final String columns;
    private final Schema schema;

    public FileStorage(String filename, String columns) {
        this.filename = filename;
        this.columns = columns;
        System.out.println("FileStorage: Creating schema with columns: " + columns);
        //todo: create the schema
        // Parse the columns string to create the schema
        this.schema = createSchema(columns);
        System.out.println("FileStorage: Created schema: " + this.schema);
        initializeFile();
    }

    private Schema createSchema(String columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Columns definition cannot be null or empty");
        }

        String[] fieldDefinitions = columns.split(",");
        List<Schema.Metadata> fields = new ArrayList<>();

        for (String fieldDefinition : fieldDefinitions) {
            String[] parts = fieldDefinition.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException("Invalid column definition: " + fieldDefinition);
            }

            String fieldName = parts[0].trim();
            String fieldType = parts[1].trim();

            // Use default lengths for simplicity
            int length = getDefaultLength(fieldType);
            fields.add(new Schema.Metadata(fieldName, fieldType, length));
        }

        Schema schema = new Schema(fields);
        System.out.println("FileStorage.createSchema: Created schema with fields: " + fields);
        return schema;
    }

    private static int getDefaultLength(String type) {
        switch (type.toLowerCase()) {
            case "string":
                return 20; // Default fixed length for strings
            case "int":
                return 4;  // Integers are 4 bytes
            case "short":
                return 2;  // Shorts are 2 bytes
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    private void initializeFile() {
        try {
            // Get table name without extension
            String tableName = filename.split("\\.")[0];

            // Create .tbl file as per spec
            String tableFile = tableName + ".tbl";
            RandomAccessFile tempFile = new RandomAccessFile(tableFile, "rw");

            // Create table with B+tree
            Table newTable = new Table(schema, tempFile, tableName, columns);
            table.put(filename, newTable);
            file.put(newTable, tempFile);

            // Initialize file with a header
            tempFile.setLength(0);
            tempFile.setLength(PAGE_SIZE);
            tempFile.seek(0);  // Move to the beginning of the file

            // Write header (16 bytes as an example)
            tempFile.writeByte(1);         // Page type (1 byte, e.g., 1 for leaf page)
            tempFile.writeShort(0);        // Number of records (2 bytes)
            tempFile.writeInt(PAGE_SIZE);  // Cell content start offset (4 bytes)
            tempFile.writeLong(0);         // Reserved/unused space (8 bytes)

            // Fill the rest of the page with zeros
            byte[] emptyPage = new byte[PAGE_SIZE - 16];
            tempFile.write(emptyPage);

            // Initialize table
            table.get(filename).initialize();

        } catch (Exception e) {
            System.out.println("Error initializing file: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public void startCSVProcess(String csvfile) {
        try {
            Table currentTable = table.get(filename);
            System.out.println("FileStorage.startCSVProcess: Retrieved table with schema: " + currentTable.getSchema());
            RandomAccessFile currentFile = file.get(currentTable);
            currentTable.processCsv(currentFile, csvfile);  // Pass both the file and filename
        } catch (Exception e) {
            System.out.println("Error processing CSV: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    public long getFileSize() {
        try {
            return file.get(table.get(filename)).length();
        } catch (IOException e) {
            throw new RuntimeException("Error getting file size: " + e.getMessage());
        }
    }

    public Schema getSchema() {
        return this.schema;
    }

    public void insertRecord(Record record) {
        try {
            Table currentTable = table.get(filename);
            if (currentTable != null) {
                currentTable.insertRecord(record);
            } else {
                throw new RuntimeException("No table selected");
            }
        } catch (Exception e) {
            throw new RuntimeException("Error inserting record: " + e.getMessage());
        }
    }
}