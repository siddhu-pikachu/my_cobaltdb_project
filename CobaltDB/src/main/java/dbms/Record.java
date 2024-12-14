package dbms;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class Record {
    private int rowId;
    private final Schema schema;
    private final Map<String, Object> values;
    private char deletionMarker;

    public Record(int rowId, Schema schema, Map<String, Object> values) {
        this.rowId = rowId;
        this.schema = schema;
        this.values = values;
        this.deletionMarker = '\u0000';

    }

    public void setRowId(int id) {
        this.rowId = id;
    }

    public void setDeletionMarker() {
        this.deletionMarker = '1';
    }

    public int getRowId() {
        return this.rowId;
    }

    public byte[] serialize() {
        List<Schema.Metadata> fields = schema.getFields();

        int bufferSize = fields.stream()
                .mapToInt(field -> field.getType().equals("string") ? field.getLength(): 4)
                .sum() + 5;

        ByteBuffer byteBuffer = ByteBuffer.allocate(bufferSize);
        byteBuffer.order(ByteOrder.BIG_ENDIAN);

        // Write all fields in fixed-length format
        byteBuffer.putInt(rowId);  // 4 bytes

        for (Schema.Metadata field : fields) {
            Object value = values.getOrDefault(field.getName(), "DEFAULT");
            switch (field.getType()) {
                case "string":
                    writeFixedLengthString(byteBuffer, value.toString(), field.getLength());
                    break;
                case "int":
                    byteBuffer.putInt((Integer) value);
                    break;
                case "short":
                    byteBuffer.putShort((Short) value);
                    break;
            }
        }
        byteBuffer.put((byte) deletionMarker); // Add deletion marker

        return byteBuffer.array();
    }

    private void writeFixedLengthString(ByteBuffer buffer, String str, int length) {
        byte[] bytes = new byte[length];
        byte[] strBytes = str.getBytes(StandardCharsets.US_ASCII);
        System.arraycopy(strBytes, 0, bytes, 0, Math.min(strBytes.length, length));
        buffer.put(bytes);
    }

    public static Record deserialize(Schema schema, byte[] data, int rowId) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        buffer.order(ByteOrder.BIG_ENDIAN);

        Map<String, Object> values = new HashMap<>();
        for (Schema.Metadata field : schema.getFields()) {
            switch (field.getType()) {
                case "string":
                    values.put(field.getName(), readFixedLengthString(buffer, field.getLength()));
                    break;
                case "int":
                    values.put(field.getName(), buffer.getInt());
                    break;
                case "short":
                    values.put(field.getName(), buffer.getShort());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + field.getType());
            }
        }

        return new Record(rowId, schema, values);
    }

    private static String readFixedLengthString(ByteBuffer buffer, int length) {
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.US_ASCII).trim();
    }
}