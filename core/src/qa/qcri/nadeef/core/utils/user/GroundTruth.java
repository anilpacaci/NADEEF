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

package qa.qcri.nadeef.core.utils.user;

import com.google.common.collect.Maps;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import qa.qcri.nadeef.core.utils.sql.DBConnectionHelper;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by apacaci on 4/11/16.
 */
public class GroundTruth {

    private ExecutionContext context;
    private String cleanTableName;
    private Schema tableSchema;

    private Map<Integer, Tuple> cleanTableMap;

    public GroundTruth(ExecutionContext context, String cleanTableName, Schema tableSchema) {
        this.context = context;
        this.cleanTableName = cleanTableName;
        this.tableSchema = tableSchema;

        this.cleanTableMap = Maps.newHashMap();
    }

    public boolean acceptFix(Fix fix) throws SQLException, NadeefDatabaseException {
        Object cleanValue = getCleanValue(fix);
        Object dirtyValue = fix.getLeft().getValue();

        // just checks whether cell is correct
        return !cleanValue.toString().equals(dirtyValue.toString());
    }

    public Object getCleanValue(Fix fix) throws SQLException, NadeefDatabaseException {
        int tupleID = fix.getLeft().getTid();

        // user interaction, simulate user interaction by checking from clean dataset, ground truth
        Tuple cleanTuple = DBConnectionHelper.getDatabaseTuple(context.getConnectionPool(), cleanTableName, tableSchema, tupleID);
        Cell cleanCell = cleanTuple.getCell(fix.getLeft().getColumn());


        Object cleanValue = cleanCell.getValue();
        return cleanValue;
    }
}
