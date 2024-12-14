package dbms;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class App {
    private static FileStorage currentStorage = null;
    private static String currentTable = null;
    private static int nextRowId = 1;

    public static void main(String[] args) {
        System.out.println("Welcome to CobaltDB! Type your commands or 'exit' to quit.");
        System.out.println("Supported commands:");
        System.out.println("  CREATE TABLE <tablename> (<column>:<type>, ...)");
        System.out.println("  INSERT INTO <tablename> VALUES (value1, value2, ...)");
        System.out.println("  .FILE <filename>");
        System.out.println("  EXIT or QUIT");

        Scanner scanner = new Scanner(System.in);

        boolean continueRunning = true;
        while (continueRunning) {
            System.out.print("dbms> ");
            String input = scanner.nextLine().trim();

            try {
                continueRunning = processCommand(input);
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
                e.printStackTrace();
            }
        }

        scanner.close();
    }

    private static boolean processCommand(String input) {
        if (input.isEmpty()) {
            return true;
        }

        if (input.startsWith(".")) {
            return handleDotCommand(input);
        }

        String[] parts = input.split("\\s+", 2);
        String command = parts[0].toUpperCase();

        switch (command) {
            case "EXIT":
            case "QUIT":
                System.out.println("Exiting CobaltDB. Goodbye!");
                return false;

            case "CREATE":
                if (parts.length > 1) {
                    handleCreateCommand(parts[1]);
                } else {
                    System.out.println("Error: Invalid CREATE command syntax");
                }
                break;

            case "INSERT":
                if (parts.length > 1) {
                    handleInsertCommand(parts[1]);
                } else {
                    System.out.println("Error: Invalid INSERT command syntax");
                }
                break;

            default:
                System.out.println("Error: Unknown command '" + command + "'");
        }

        return true;
    }

    private static boolean handleDotCommand(String input) {
        String[] parts = input.split("\\s+", 2);
        String command = parts[0].toLowerCase();

        switch (command) {
            case ".file":
                if (parts.length > 1) {
                    String filename = parts[1];
                    try {
                        currentStorage = new FileStorage(filename, null);
                        currentTable = filename.split("\\.")[0];
                        currentStorage.startCSVProcess(filename);
                        System.out.println("Successfully processed file: " + filename);
                    } catch (Exception e) {
                        System.out.println("Error processing file: " + e.getMessage());
                    }
                } else {
                    System.out.println("Error: Filename required");
                }
                break;

            default:
                System.out.println("Error: Unknown dot command '" + command + "'");
        }
        return true;
    }

    private static void handleCreateCommand(String args) {
        Pattern pattern = Pattern.compile("TABLE\\s+(\\w+)\\s*\\((.+)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(args);

        if (matcher.find()) {
            String tableName = matcher.group(1);
            String columnsStr = matcher.group(2).replaceAll("\\s+", "");

            try {
                currentStorage = new FileStorage(tableName + ".tbl", columnsStr);
                currentTable = tableName;
                System.out.println("Table " + tableName + " created successfully");
            } catch (Exception e) {
                System.out.println("Error creating table: " + e.getMessage());
            }
        } else {
            System.out.println("Error: Invalid CREATE TABLE syntax");
            System.out.println("Correct syntax: CREATE TABLE tablename (column:type, column:type, ...)");
        }
    }

    private static void handleInsertCommand(String args) {
        Pattern pattern = Pattern.compile("INTO\\s+(\\w+)\\s+VALUES\\s*\\((.+)\\)", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(args);

        if (matcher.find()) {
            String tableName = matcher.group(1);
            String valuesStr = matcher.group(2);

            if (currentStorage == null || !tableName.equals(currentTable)) {
                System.out.println("Error: Table " + tableName + " not found or not selected");
                return;
            }

            try {
                // Parse values
                List<String> values = new ArrayList<>();
                StringBuilder currentValue = new StringBuilder();
                boolean inQuotes = false;

                for (char c : valuesStr.toCharArray()) {
                    if (c == ',' && !inQuotes) {
                        values.add(currentValue.toString().trim());
                        currentValue = new StringBuilder();
                    } else if (c == '"' || c == '\'') {
                        inQuotes = !inQuotes;
                    } else {
                        currentValue.append(c);
                    }
                }
                values.add(currentValue.toString().trim());

                // Create record from values
                Map<String, Object> recordValues = new HashMap<>();
                Schema schema = currentStorage.getSchema();
                List<Schema.Metadata> fields = schema.getFields();

                if (values.size() != fields.size()) {
                    throw new IllegalArgumentException("Number of values doesn't match number of columns");
                }

                for (int i = 0; i < fields.size(); i++) {
                    Schema.Metadata field = fields.get(i);
                    String value = values.get(i);

                    switch (field.getType()) {
                        case "int":
                            recordValues.put(field.getName(), Integer.parseInt(value));
                            break;
                        case "string":
                            recordValues.put(field.getName(), value);
                            break;
                        // Add more types as needed
                    }
                }

                // Create and insert record
                Record record = new Record(nextRowId++, schema, recordValues);
                currentStorage.insertRecord(record);
                System.out.println("Record inserted successfully");

            } catch (Exception e) {
                System.out.println("Error inserting values: " + e.getMessage());
                e.printStackTrace();
            }
        } else {
            System.out.println("Error: Invalid INSERT syntax");
            System.out.println("Correct syntax: INSERT INTO tablename VALUES (value1, value2, ...)");
        }
    }
}