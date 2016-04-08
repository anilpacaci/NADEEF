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

import com.google.common.collect.Sets;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Logger;
import qa.qcri.nadeef.tools.PerfReport;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;

/**
 * This class is responsible of updating database with suggested values as well as maintaining Audit table
 * Created by y997zhan on 4/8/16.
 */
public class AuditManager {

    private static Logger tracer = Logger.getLogger(AuditManager.class);


    private ExecutionContext context;

    private Set<Cell> changedCells;

    public AuditManager(ExecutionContext context) {
        this.context = context;
        this.changedCells = Sets.newHashSet();
    }

    public boolean isAlreadyUpdated(Cell cell) {
        return this.changedCells.contains(cell);
    }


    /**
     * Directly applies given fix to database. First check {{@link #isAlreadyUpdated(Cell)}} to make sure that this cell is not previously updated
     *
     * @param fix
     */
    public void applyFix(Fix fix) throws NadeefDatabaseException, SQLException {
        DBConnectionPool connectionPool = context.getConnectionPool();
        Connection nadeefConn = null;
        Connection sourceConn = null;
        Statement sourceStat = null;
        PreparedStatement auditStat = null;

        String auditTableName = NadeefConfiguration.getAuditTableName();
        String rightValue;
        String oldValue;

        int count = 0;

        try {
            nadeefConn = connectionPool.getNadeefConnection();
            sourceConn = connectionPool.getSourceConnection();
            sourceStat = sourceConn.createStatement();
            auditStat =
                nadeefConn.prepareStatement(
                    "INSERT INTO " + auditTableName +
                        " VALUES (default, ?, ?, ?, ?, ?, ?, current_timestamp)");
            Cell cell = fix.getLeft();
            Object oldValue_ = cell.getValue();
            if (oldValue_ == null) {
                oldValue = null;
            } else {
                oldValue = oldValue_.toString();
            }

            rightValue = fix.getRightValue();

            // check for numerical type.
            if (rightValue != null && !CommonTools.isNumericalString(rightValue)) {
                rightValue = '\'' + rightValue + '\'';
            }

            if (oldValue != null && !CommonTools.isNumericalString(oldValue)) {
                oldValue = '\'' + oldValue + '\'';
            }

            Column column = cell.getColumn();
            String tableName = column.getTableName();
            String updateSql =
                "UPDATE " + tableName +
                    " SET " + column.getColumnName() + " = " + rightValue +
                    " WHERE tid = " + cell.getTid();
            tracer.fine(updateSql);

            auditStat.setInt(1, fix.getVid());
            auditStat.setInt(2, cell.getTid());
            auditStat.setString(3, column.getTableName());
            auditStat.setString(4, column.getColumnName());
            auditStat.setString(5, oldValue);
            auditStat.setString(6, rightValue);

            sourceStat.executeUpdate(updateSql);
            auditStat.executeUpdate();
            nadeefConn.commit();
            sourceConn.commit();

            count++;
            // add cell to hash set
            this.changedCells.add(fix.getLeft());

            PerfReport.appendMetric(PerfReport.Metric.UpdatedCellNumber, count);
        } finally {
            if (auditStat != null) {
                auditStat.close();
            }

            if (sourceStat != null) {
                sourceStat.close();
            }

            if (nadeefConn != null) {
                nadeefConn.close();
            }

            if (sourceConn != null) {
                sourceConn.close();
            }
        }


    }


}
