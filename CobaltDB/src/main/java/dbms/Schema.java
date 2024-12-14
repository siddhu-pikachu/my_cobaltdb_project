package dbms;

import java.util.*;

public class Schema {
    public static class Metadata {
        private final String name;  // Field name
        private final String type;  // Field type as a string: "int", "string", etc.
        private final int length;   // Field length for fixed-length types

        // Constructor
        public Metadata(String name, String type, int length) {
            this.name = name;
            this.type = type.toLowerCase();
            this.length = length;
        }

        public String getName() {
            return name;
        }

        public String getType() {
            return type;
        }

        public int getLength() {
            return length;
        }

        @Override
        public String toString() {
            return "Metadata{name='" + name + "', type='" + type + "', length=" + length + "}";
        }
    }

    private final List<Metadata> fields;

    public Schema(List<Metadata> fields) {
        this.fields = fields;
    }

    public List<Metadata> getFields() {
        return fields;
    }

    public Metadata getField(String fieldName) {
        return fields.stream()
                .filter(f -> f.getName().equals(fieldName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Field not found: " + fieldName));
    }

    @Override
    public String toString() {
        return "Schema{" + "fields=" + fields + '}';
    }
}