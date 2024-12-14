# DavisBase Implementation Documentation

## Overview
This document details the implementation of a simplified database management system based on the DavisBase specification. The system implements a basic SQL-like interface with support for table creation, record insertion, and a B+tree-based storage system.

## System Architecture

### Core Components

1. **App.java (Main Interface)**
  - Provides the command-line interface
  - Handles user input parsing
  - Manages command routing and execution
  - Implements core commands: CREATE TABLE, INSERT INTO, .FILE

2. **Storage Layer**
  - **FileStorage.java**: Manages physical file operations and table management
  - **Page.java**: Implements page-level operations and structure
  - **BPlusTree.java**: Provides B+tree implementation for indexing
  - **Table.java**: Manages table-level operations

3. **Data Management**
  - **Record.java**: Handles record serialization and deserialization
  - **Schema.java**: Manages table schema and metadata
  - **ColumnType.java & DataType.java**: Define and handle data types

## Implementation Details

### File Structure
- Uses a page-based storage system with 512-byte pages
- Each table is stored in a separate .tbl file
- Pages are structured with:
  - 16-byte header
  - Cell offset array
  - Cell content area

### B+Tree Implementation
The B+tree implementation provides:
- Efficient record insertion and retrieval
- Automatic page splitting when full
- Maintenance of sorted order by rowid
- Support for both leaf and internal nodes

### Record Format
Records are stored with:
- 1-byte payload size
- 4-byte rowid
- Variable-length data section
- Deletion marker

### Page Types
Implements four page types:
- Table Leaf (0x0D)
- Table Interior (0x05)
- Index Leaf (0x0A)
- Index Interior (0x02)

## Key Features

### 1. Table Creation
- Supports CREATE TABLE with column definitions
- Handles multiple data types (string, int)
- Automatically generates schema metadata

### 2. Record Insertion
- Implements INSERT INTO command
- Maintains B+tree structure
- Handles automatic page splits
- Generates monotonically increasing rowids

### 3. File Management
- Supports .FILE command for CSV processing
- Manages file growth and page allocation
- Maintains data consistency

## Data Types Supported
- INT (4 bytes)
- STRING (variable length)
- Additional types can be easily added through the DataType enum

## Sample Usage

```sql
CREATE TABLE tabel (name:string, type:string);
INSERT INTO tabel VALUES (a,b);
.FILE example.csv;
```

## Technical Specifications

### Storage Format
- Page Size: 512 bytes
- Header Size: 16 bytes
- Maximum Records per Page: Variable (depends on record size)
- File Extension: .tbl

### B+Tree Properties
- Order: 4 (default)
- Split Strategy: Right-heavy
- Key Type: 32-bit integer (rowid)

## Performance Considerations
- Page splits are optimized for sequential inserts
- Record format minimizes internal fragmentation
- B+tree structure ensures O(log n) operations

## Limitations
- No support for DELETE or UPDATE operations
- Limited data type support
- No transaction management
- Single-user system

## Future Enhancements
1. Implementation of DELETE and UPDATE operations
2. Support for additional data types
3. Multi-user concurrency control
4. Transaction management
5. Query optimization

## Conclusion
This implementation provides a functional foundation for a simple database system, demonstrating core concepts of database management including file organization, indexing, and record management. While limited in scope, it successfully implements the core requirements of the DavisBase specification.