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

package qa.qcri.nadeef.core.utils.sql;

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by apacaci on 4/11/16.
 */
public class DBConnectionHelper {

    private static Logger tracer = Logger.getLogger(DBConnectionHelper.class);


    /**
     * Reads the value of a cell and constructs a cell object
     * @param dbConnectionPool
     * @param tableName Dirty or Clean Data source. It only works for source databases imported to Nadeef
     * @param tupleID
     * @param attribute
     * @return
     * @throws SQLException
     * @throws NadeefDatabaseException
     */
    public static Cell getDatabaseCell(DBConnectionPool dbConnectionPool, String tableName, int tupleID, String attribute) throws SQLException, NadeefDatabaseException {
        Cell.Builder builder = new Cell.Builder();

        SQLDialectBase dialectManager = SQLDialectFactory.getDialectManagerInstance(dbConnectionPool.getSourceDBConfig().getDialect());
        Connection conn = null;
        Statement stat = null;
        Object value = null;

        try {
            conn = dbConnectionPool.getSourceConnection();
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(dialectManager.selectCell(tableName, tupleID, attribute));
            if(rs.next()) {
                value = rs.getObject(attribute);
                builder.tid(tupleID).column(new Column(tableName, attribute)).value(value);
            }
            rs.close();
        } catch ( SQLException e) {
            tracer.error("Cell value could NOT be retrieved from database", e);
            throw new NadeefDatabaseException("Cell with tid:" + tupleID + " attribute: " + attribute + "could NOT be read", e);
        } finally {
            if(stat != null && !stat.isClosed()) {
                stat.close();
            }
            if(conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        return builder.build();
    }

    /**
     * Reads given tuple from database and constructs a Tuple Object
     * @param dbConnectionPool
     * @param tableName Dirty or Clean Data source. It only works for source databases imported to Nadeef
     * @param tupleID
     * @return
     * @throws SQLException
     * @throws NadeefDatabaseException
     */
    public static Tuple getDatabaseTuple(DBConnectionPool dbConnectionPool, String tableName, Schema tableSchema, int tupleID ) throws SQLException, NadeefDatabaseException{
        Cell.Builder builder = new Cell.Builder();

        SQLDialectBase dialectManager = SQLDialectFactory.getDialectManagerInstance(dbConnectionPool.getSourceDBConfig().getDialect());
        Connection conn = null;
        Statement stat = null;
        Tuple tuple = null;

        try {
            conn = dbConnectionPool.getSourceConnection();
            stat = conn.createStatement();
            ResultSet rs = stat.executeQuery(dialectManager.selectTuple(tableName, tupleID));
            if(rs.next()) {
                List<byte[]> values = new ArrayList<>();
                for(Column column : tableSchema.getColumns()) {
                    values.add(rs.getBytes(column.getColumnName()));
                }
                tuple = new Tuple(tupleID, tableSchema,values);
            }
            rs.close();
        } catch (Exception e) {
            tracer.error("Tuple could NOT be retrieved from database", e);
            throw new NadeefDatabaseException("Tuple with tid:" + tupleID + " could NOT be read", e);
        } finally {
            if(stat != null && !stat.isClosed()) {
                stat.close();
            }
            if(conn != null && !conn.isClosed()) {
                conn.close();
            }
        }
        return tuple;
    }
}
