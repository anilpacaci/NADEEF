/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * Table attribute object, used as the operand in the rule hint.
 */
public class Cell {
    private String tableName;
    private String attributeName;
    private String schemaName;

    /**
     * Constructor.
     */
    public Cell(String tableName, String attributeName) {
        this("public", tableName, attributeName);
    }

    /**
     * Constructor.
     * @param schemaName schema.
     * @param tableName table.
     * @param attributeName attribute.
     */
    public Cell(String schemaName, String tableName, String attributeName) {
        if (attributeName == null || tableName == null || schemaName == null) {
            throw new IllegalArgumentException("Attribute name cannot be null.");
        }
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.attributeName = attributeName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getAttributeName() {
        return attributeName;
    }

    /**
     * Generates a string with format of 'schemaName'.'tableName'.'atrributeName'.
     */
    public String getFullAttributeName() {
        StringBuilder result = new StringBuilder();
        boolean hasPrefix = false;
        if (schemaName != null && !schemaName.isEmpty()) {
            result.append(schemaName);
            hasPrefix = true;
        }

        if (tableName != null && !tableName.isEmpty()) {
            if (hasPrefix) {
                result.append('.');
            }
            result.append(tableName);
            hasPrefix = true;
        }

        if (hasPrefix) {
            result.append('.');
        }
        result.append(attributeName);
        return result.toString();
    }

    /**
     * Generates a string with format of 'schemaName'.'tableName'.
     */
    public String getFullTableName() {
        StringBuilder result = new StringBuilder();
        boolean hasPrefix = false;
        if (schemaName != null && !schemaName.isEmpty()) {
            result.append(schemaName);
            hasPrefix = true;
        }

        if (tableName != null && !tableName.isEmpty()) {
            if (hasPrefix) {
                result.append('.');
            }
            result.append(tableName);
        }

        return result.toString();
    }

    //<editor-fold desc="Custom equal / hashcode">
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (obj == null || !(obj instanceof Cell)) {
            return false;
        }

        Cell cell = (Cell)obj;
        if (cell.getFullAttributeName() != this.getFullAttributeName()) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        final int root = 109;
        return root * tableName.hashCode() * attributeName.hashCode() * schemaName.hashCode();
    }
    //</editor-fold>

}