package dbms;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

public class BPlusTree {
    private Node root;
    private boolean isInitialized;
    private int nextPageNumber;
    private final RandomAccessFile file;
    private final int order;
    private final Schema schema;

    public BPlusTree(Schema schema, RandomAccessFile file) {
        this(schema, file, 4);  // Default order of 4
    }

    public BPlusTree(Schema schema, RandomAccessFile file, int order) {
        System.out.println("BPlusTree constructor: Received schema: " + schema);
        this.file = file;
        this.schema = schema;
        this.order = order;
        this.isInitialized = false;
        this.nextPageNumber = 0;

        if (this.schema == null) {
            throw new IllegalArgumentException("Schema cannot be null in BPlusTree constructor");
        }
    }

    private void initialize() throws IOException {
        if (!isInitialized) {
            System.out.println("BPlusTree.initialize: Creating LeafNode with schema: " + schema);
            root = new LeafNode(schema, assignPageNumber(), file);
            isInitialized = true;
        }
    }

    private LeafNode findTargetPage(int rowId) {
        Node currentNode = root;
        while (!currentNode.isLeaf()) {
            InternalNode internal = (InternalNode) currentNode;
            int position = binarySearch(internal.getKeys(), rowId, internal.getNumKeys());
            int childIndex = position < internal.getNumKeys() ? position : internal.getNumKeys();
            currentNode = internal.getChildren()[childIndex];
        }
        return (LeafNode) currentNode;
    }

    public boolean insert(Record record) throws IOException {
        System.out.println("Attempting to insert record with rowId: " + record.getRowId());
        initialize();

        LeafNode targetPage = findTargetPage(record.getRowId());
        System.out.println("Found target page: " + targetPage.getPageNumber());

        byte[] recordData = record.serialize();
        System.out.println("Record serialized, size: " + recordData.length);
        System.out.println("Serialized Record: " + Arrays.toString(recordData));

        if (targetPage.getPage().hasSpace(recordData.length)) {
            boolean success = targetPage.getPage().addRecord(record);
            System.out.println("Insert " + (success ? "successful" : "failed"));
            return success;
        } else {
            System.out.println("Page full, need to split");
            splitLeafNode(targetPage, record);
            return true;
        }
    }

    private void splitLeafNode(LeafNode leaf, Record newRecord) throws IOException {
        // Create new leaf node
        LeafNode newLeaf = new LeafNode(schema, assignPageNumber(), file);
        System.out.println("Created new leaf node with page number: " + newLeaf.getPageNumber());

        // Get all records including new one
        Record[] currentRecords = leaf.getPage().getAllRecords();
        Record[] allRecords = new Record[currentRecords.length + 1];
        System.arraycopy(currentRecords, 0, allRecords, 0, currentRecords.length);
        allRecords[currentRecords.length] = newRecord;

        // Sort records by rowId
        Arrays.sort(allRecords, (r1, r2) -> Integer.compare(r1.getRowId(), r2.getRowId()));
        int splitPoint = allRecords.length / 2;

        // Clear old leaf and redistribute records
        leaf.getPage().clear();
        newLeaf.getPage().clear();

        // Fill first half in ascending order (for bottom-up)
        for (int i = 0; i < splitPoint; i++) {
            leaf.getPage().addRecord(allRecords[i]);
        }

// Fill second half in ascending order (for bottom-up)
        for (int i = splitPoint; i < allRecords.length; i++) {
            newLeaf.getPage().addRecord(allRecords[i]);
        }

        // Update leaf chain pointers
        Integer oldRightSibling = leaf.getPage().getRightSibling();
        newLeaf.getPage().setRightSibling(oldRightSibling != null ? oldRightSibling : -1);
        leaf.getPage().setRightSibling(newLeaf.getPageNumber());
        System.out.println("Split complete - Left node: " + leaf.getPageNumber() +
                " now points to: " + newLeaf.getPageNumber() +
                " which points to: " + (oldRightSibling != null ? oldRightSibling : "none"));

        // Insert split key into parent
        int splitKey = allRecords[splitPoint].getRowId();
        insertIntoParent(leaf, splitKey, newLeaf);
    }

    private void insertIntoParent(Node leftNode, int key, Node rightNode) throws IOException {
        if (root == leftNode) {
            InternalNode newRoot = new InternalNode(assignPageNumber(), file);

            // Create a new Page for the interior node with correct constructor order
            Page interiorPage = new Page(schema, file, newRoot.getPageNumber(), (byte)0x05);
            interiorPage.initialize();  // This sets up the page header

            // Write the interior node data
            interiorPage.writeInteriorCell(leftNode.getPageNumber(), key);
            interiorPage.setRightChild(rightNode.getPageNumber());

            // Update in-memory structure
            newRoot.getKeys()[0] = key;
            newRoot.getChildren()[0] = leftNode;
            newRoot.getChildren()[1] = rightNode;
            newRoot.setNumKeys(1);
            root = newRoot;

            // Update parent pointers
            updateNodeParent(leftNode, newRoot.getPageNumber());
            updateNodeParent(rightNode, newRoot.getPageNumber());

            System.out.println("Created new root " + newRoot.getPageNumber() +
                    " with children " + leftNode.getPageNumber() +
                    " and " + rightNode.getPageNumber());
        } else {
            Node parent = findParent(leftNode);
            System.out.println("Found parent " + parent.getPageNumber() + " for node " + leftNode.getPageNumber());

            InternalNode parentNode = (InternalNode) parent;
            if (parentNode.getNumKeys() < order - 1) {
                insertIntoInternal(parentNode, key, rightNode);
            } else {
                splitInternalNode(parentNode, key, rightNode);
            }
        }
    }

    private void updateNodeParent(Node node, int parentPageNum) throws IOException {
        if (node instanceof LeafNode) {
            LeafNode leaf = (LeafNode) node;
            leaf.getPage().setParent(parentPageNum);
            System.out.println("Updated parent of leaf " + node.getPageNumber() +
                    " to " + parentPageNum);
        } else if (node instanceof InternalNode) {
            InternalNode internal = (InternalNode) node;
            internal.setParent(parentPageNum);
            System.out.println("Updated parent of internal node " + node.getPageNumber() +
                    " to " + parentPageNum);
        }
    }

    private void insertIntoInternal(InternalNode node, int key, Node rightChild) {
        int pos = 0;
        while (pos < node.getNumKeys() && node.getKeys()[pos] < key) {
            pos++;
        }

        // Shift existing keys and children
        for (int i = node.getNumKeys() - 1; i >= pos; i--) {
            node.getKeys()[i + 1] = node.getKeys()[i];
            node.getChildren()[i + 2] = node.getChildren()[i + 1];
        }

        node.getKeys()[pos] = key;
        node.getChildren()[pos + 1] = rightChild;
        node.setNumKeys(node.getNumKeys() + 1);
    }

    private void splitInternalNode(InternalNode node, int newKey, Node newChild) throws IOException {
        int[] tempKeys = new int[order];
        Node[] tempChildren = new Node[order + 1];

        // Find position for new key
        int pos = 0;
        while (pos < node.getNumKeys() && node.getKeys()[pos] < newKey) {
            pos++;
        }

        // Copy to temp arrays
        System.arraycopy(node.getKeys(), 0, tempKeys, 0, pos);
        System.arraycopy(node.getChildren(), 0, tempChildren, 0, pos);
        tempKeys[pos] = newKey;
        tempChildren[pos] = node.getChildren()[pos];
        tempChildren[pos + 1] = newChild;
        System.arraycopy(node.getKeys(), pos, tempKeys, pos + 1, node.getNumKeys() - pos);
        System.arraycopy(node.getChildren(), pos + 1, tempChildren, pos + 2, node.getNumKeys() - pos);

        // Create new internal node
        InternalNode newNode = new InternalNode(assignPageNumber(), file);

        // Find middle key that will be promoted
        int mid = (order - 1) / 2;
        int promoteKey = tempKeys[mid];

        // Copy first half to original node
        node.setNumKeys(mid);
        System.arraycopy(tempKeys, 0, node.getKeys(), 0, mid);
        System.arraycopy(tempChildren, 0, node.getChildren(), 0, mid + 1);

        // Copy second half to new node
        newNode.setNumKeys(order - 1 - mid - 1);
        System.arraycopy(tempKeys, mid + 1, newNode.getKeys(), 0, newNode.getNumKeys());
        System.arraycopy(tempChildren, mid + 1, newNode.getChildren(), 0, newNode.getNumKeys() + 1);

        // Promote middle key to parent
        insertIntoParent(node, promoteKey, newNode);
    }

    private Node findParent(Node node) throws IOException {
        if (node == root) {
            throw new IOException("Cannot find parent of root");
        }

        Node current = root;
        System.out.println("Finding parent for node " + node.getPageNumber() + ", starting at root " + root.getPageNumber());

        while (!current.isLeaf()) {
            InternalNode internal = (InternalNode) current;

            // Check if any child is our target node
            for (int i = 0; i <= internal.getNumKeys(); i++) {
                if (internal.getChildren()[i] == node) {
                    System.out.println("Found parent " + current.getPageNumber() + " for node " + node.getPageNumber());
                    return current;
                }
            }

            // If not found, traverse down using first key comparison
            int firstKey;
            if (node.isLeaf()) {
                LeafNode leaf = (LeafNode) node;
                Record[] records = leaf.getPage().getAllRecords();
                firstKey = records.length > 0 ? records[0].getRowId() : 0;
            } else {
                InternalNode internalNode = (InternalNode) node;
                firstKey = internalNode.getNumKeys() > 0 ? internalNode.getKeys()[0] : 0;
            }

            // Find appropriate child
            int i = 0;
            while (i < internal.getNumKeys() && internal.getKeys()[i] <= firstKey) {
                i++;
            }
            current = internal.getChildren()[i];
        }

        throw new IOException("Parent not found for node " + node.getPageNumber());
    }

    private int binarySearch(int[] keys, int rowId, int numKeys) {
        int left = 0;
        int right = numKeys - 1;

        while (left <= right) {
            int mid = (left + right) / 2;
            if (keys[mid] == rowId) return mid;
            if (keys[mid] < rowId) left = mid + 1;
            else right = mid - 1;
        }
        return left;
    }

    private int assignPageNumber() {
        return nextPageNumber++;
    }
}