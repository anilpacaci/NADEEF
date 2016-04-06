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

package qa.qcri.nadeef.core.utils.classification;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.core.utils.sql.SQLDialectBase;
import qa.qcri.nadeef.core.utils.sql.SQLDialectFactory;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Created by apacaci on 4/5/16.
 */
public class ClassificationHelper {

    private static Logger tracer = Logger.getLogger(ClassificationHelper.class);

    /**
     * For a given table and column, it reads all distinct values into a List<String>
     *     it is used for the classificaiton. For categorical values, we read all distinct values and create a nominal feature for classifier
     * @param dbConfig
     * @param tableName
     * @param attributeName
     * @return
     * @throws SQLException
     * @throws NadeefDatabaseException
     */
    public static List<String> getDistinctValues(DBConfig dbConfig, String tableName, String attributeName ) throws NadeefDatabaseException {
        List<String> result = Lists.newArrayList();

        SQLDialectBase dialectManager = SQLDialectFactory.getDialectManagerInstance(dbConfig.getDialect());
        Statement stat = null;
        Object value = null;

        try (Connection conn = DBConnectionPool.createConnection(dbConfig)) {
            stat = conn.createStatement();
            String queryString = new StringBuilder().append("SELECT DISTINCT ").append(attributeName).append(" FROM ").append(tableName).toString();
            ResultSet rs = stat.executeQuery(queryString);
            while(rs.next()) {
                result.add(rs.getString(attributeName));
            }

            rs.close();
        } catch (IllegalAccessException | InstantiationException | SQLException | ClassNotFoundException e) {
            tracer.error("All values could NOT be retrieved from database", e);
            throw new NadeefDatabaseException("Table:" + tableName + " attribute: " + attributeName + "could NOT be read", e);
        }

        return result;
    }
}
