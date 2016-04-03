/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.tools.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Fix extension class.
 *
 */
public class Fixes {
    /**
     * Generates fix id from database. This methods does not close the connection.
     * @param connectionPool ConnectionPool.
     * @return id.
     */
    // TODO:
    // This doesn't promise that you will always get the right id in
    // concurrency mode, design a better way of generating it.
    public static int generateFixId(DBConnectionPool connectionPool) {
        Logger tracer = Logger.getLogger(Fix.class);
        Connection conn = null;
        Statement stat = null;
        try {
            String tableName = NadeefConfiguration.getRepairTableName();
            conn = connectionPool.getNadeefConnection();
            stat = conn.createStatement();
            ResultSet resultSet =
                stat.executeQuery("SELECT MAX(id) + 1 as id from " + tableName);
            conn.commit();
            int result = -1;
            if (resultSet.next()) {
                result = resultSet.getInt("id");
            }
            stat.close();
            return result;
        } catch (Exception ex) {
            tracer.error("Unable to generate Fix id.", ex);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }

                if (stat != null) {
                    stat.close();
                }
            } catch (SQLException ex) {}
        }
        return 0;
    }

    public static Collection<Fix> fromQuery(ResultSet resultSet) {
        Preconditions.checkNotNull(resultSet);
        List<Fix> result = Lists.newArrayList();
        Cell.Builder cellBuilder = new Cell.Builder();
        try {
            while (resultSet.next()) {
                int vid = resultSet.getInt("vid");
                int op = resultSet.getInt("op");
                int c1TupleId = resultSet.getInt("c1_tupleid");
                Fix.Builder builder = new Fix.Builder();

                String c1TableName = resultSet.getString("c1_tablename");
                String c1Attribute = resultSet.getString("c1_attribute" );
                String c1Value = resultSet.getString("c1_value" );
                int c2TupleId = resultSet.getInt("c2_tupleid");
                String c2TableName = resultSet.getString("c2_tablename");
                String c2Attribute = resultSet.getString("c2_attribute");
                String c2Value = resultSet.getString("c2_value");
                Cell c1Cell =
                    cellBuilder.column(new Column(c1TableName, c1Attribute))
                        .value(c1Value)
                        .tid(c1TupleId)
                        .build();
                Fix newFix = null;
                // TODO: support different type of operations
                if (c2TableName != null) {
                    Cell c2Cell =
                        cellBuilder.column(
                            new Column(c2TableName, c2Attribute)
                        ).value(c2Value).tid(c2TupleId).build();
                    newFix =
                        builder
                            .vid(vid)
                            .left(c1Cell)
                            .right(c2Cell)
                            .op(Operation.values()[op])
                            .build();
                } else {
                    newFix =
                        builder
                            .vid(vid)
                            .left(c1Cell)
                            .right(c2Value)
                            .op(Operation.values()[op])
                            .build();
                }

                result.add(newFix);
            }
        } catch (SQLException e) {
            Logger tracer = Logger.getLogger(Fixes.class);
            tracer.error("Exceptions happen during parsing Fixes.", e);
        }
        return result;
    }

    /**
     * Pre-processing stage for a repair of single cell, modifies cell in RHS with values, makes them constant
     * @param cell Cell to be repaired
     * @param originalFixes
     * @return list of Fix where all LHS is cell, all RHS is replaced with constants
     */
    public static Collection<Fix> substituteRHS(Cell cell, Collection<Fix> originalFixes) {
        List<Fix> modifiedFixes = new ArrayList<>();
        String attribute = cell.getColumn().getColumnName();
        int tupleID = cell.getTid();


        for(Fix originalFix : originalFixes) {
            if(originalFix.getLeft().equals(cell)) {
                // LHS is already this cell, replace RHS with its value
                Fix newFix = new Fix.Builder().left(cell).right(originalFix.getRight().getValue().toString()).op(originalFix.getOperation()).vid(originalFix.getVid()).build();
                modifiedFixes.add(newFix);
            } else if(originalFix.getRight().equals(cell)) {
                // RHS is the cell, swap first, then replace RHS with constant value
                Fix newFix = new Fix.Builder().left(cell).right(originalFix.getLeft().getValue().toString()).op(originalFix.getOperation()).vid(originalFix.getVid()).build();
                modifiedFixes.add(newFix);
            } else {
                // neither RHS or LHS is the cell, skip this
            }
        }

        return modifiedFixes;
    }
 }
