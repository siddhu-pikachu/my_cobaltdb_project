package dbms;

import java.io.*;
import java.util.*;

public class Table {
    private final String tableName;
    private final BPlusTree bPlusTree;
    private int nextRowId;
    private final RandomAccessFile file;
    private final String columns;
    private final Schema schema;

    public Table(Schema schema, RandomAccessFile file, String name, String columns) {
        this.tableName = name;
        this.file = file;
        this.schema = schema;
        this.columns = columns;
        System.out.println("Table constructor: Creating BPlusTree with schema: " + this.schema);
        this.bPlusTree = new BPlusTree(schema, file);
        this.nextRowId = 0;
    }

    public void initialize() throws IOException {
        file.setLength(512);
        System.out.println("Initialized table file with size: " + file.length());
    }

    public void processCsv(RandomAccessFile dbFile, String csvFilePath) {
        System.out.println("Starting to process CSV file: " + csvFilePath);
        try {
            // Create a File object to check if the CSV exists
            File csvFile = new File(csvFilePath);
            System.out.println("CSV file exists: " + csvFile.exists());
            System.out.println("CSV file absolute path: " + csvFile.getAbsolutePath());

            if (!csvFile.exists()) {
                throw new FileNotFoundException("CSV file not found: " + csvFilePath);
            }

            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            System.out.println("Successfully opened CSV file");

            String line = br.readLine(); // skip header
            System.out.println("Header line: " + line);

            line = br.readLine();
            int recordCount = 0;

            while (line != null) {
                System.out.println("Processing line: " + line);
                int rId = assignRowId();
                Record empRecord = parseCSVLine(line, rId);
                System.out.println("Created record with rowId: " + rId);
                System.out.println("Parsed Record: " + empRecord);
                System.out.println("Serialized Record: " + Arrays.toString(empRecord.serialize()));


                if (bPlusTree.insert(empRecord)) {
                    System.out.println("Successfully inserted record " + rId);
                    recordCount++;
                } else {
                    System.out.println("Failed to insert record with rowId " + rId);
                }

                line = br.readLine();
                if (line != null) {
                    System.out.println("CSV Line Read: " + line); // Debug
                }
            }

            System.out.println("Finished processing CSV. Total records processed: " + recordCount);
            br.close();

            // Verify file size after processing
            System.out.println("Final table file size: " + dbFile.length());

        } catch (Exception e) {
            System.out.println("Error processing CSV: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private char toChar(String s) {
        return s.length() == 1 ? s.charAt(0) : '\u0000';
    }

    private short toShort(String s) {
        try {
            int value = Integer.parseInt(s);
            if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                return (short) value;
            }
        } catch (NumberFormatException ignored) {
            System.out.println("Warning: Could not parse short value: " + s);
        }
        return 0;
    }

    private String removeDash(String s) {
        return s.substring(0, 3) + s.substring(4, 6) + s.substring(7, 11);
    }

    private Record parseCSVLine(String line, int rId) {
        try {
            String[] attributes = line.split(",");
            System.out.println("Parsing line with " + attributes.length + " attributes");

            // Validate the number of attributes matches the schema
            if (attributes.length != schema.getFields().size()) {
                throw new IllegalArgumentException("Mismatch between schema and CSV attributes count");
            }

            // Parse attributes dynamically based on schema
            Map<String, Object> values = new HashMap<>();
            List<Schema.Metadata> fields = schema.getFields();

            for (int i = 0; i < fields.size(); i++) {
                Schema.Metadata field = fields.get(i);
                String rawValue = attributes[i].trim();

                // Parse based on field type
                switch (field.getType()) {
                    case "string":
                        values.put(field.getName(), rawValue);
                        break;
                    case "char":
                        values.put(field.getName(), toChar(rawValue));
                        break;
                    case "int":
                        values.put(field.getName(), Integer.parseInt(rawValue));
                        break;
                    case "short":
                        values.put(field.getName(), toShort(rawValue));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported field type: " + field.getType());
                }
            }

            // Create the record using the schema and parsed values
            return new Record(rId, schema, values);
        } catch (Exception e) {
            System.out.println("Error parsing line: " + line);
            e.printStackTrace();
            throw e;
        }
    }

    private int assignRowId() {
        return ++nextRowId;
    }

    public String getSchema() {
        return schema.toString();
    }

    public void insertRecord(Record record) throws IOException {
        if (!bPlusTree.insert(record)) {
            throw new IOException("Failed to insert record");
        }
    }
}