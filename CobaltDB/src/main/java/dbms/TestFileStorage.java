package dbms;

public class TestFileStorage {
    public static void main(String[] args) {
        try {
            // Define schema with explicit string length
            String columns = "id:int,name:string,age:int";  // Added length for string
            System.out.println("Creating FileStorage with columns: " + columns);

            // Initialize FileStorage
            FileStorage storage = new FileStorage("employee.csv", columns);

            System.out.println("Processing CSV data...");
            storage.startCSVProcess("employee.csv");

            System.out.println("File size after inserts: " + storage.getFileSize() + " bytes");

        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}