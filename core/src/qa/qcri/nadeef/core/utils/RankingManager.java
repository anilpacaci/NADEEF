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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Logger;

import java.sql.*;
import java.util.*;

/**
 * It is used to rank updates (VOI - F1/F2 etc).
 * Created by apacaci on 4/7/16.
 */
public class RankingManager {

    private static Logger tracer = Logger.getLogger(RankingManager.class);


    private ExecutionContext context;
    private String dirtyTableName;
    private String cleanTableName;
    private List<RepairGroup> repairGroups;
    private Set<String> finishedGroups;

    public RankingManager(ExecutionContext context, String dirtyTableName, String cleanTableName) {
        this.context = context;
        this.dirtyTableName = dirtyTableName;
        this.cleanTableName = cleanTableName;
        this.repairGroups = Lists.newArrayList();
        this.finishedGroups= Sets.newHashSet();
    }


    public RepairGroup getTopGroup() throws NadeefDatabaseException {
        repairGroups.clear();
        DBConfig dbConfig = context.getConnectionPool().getNadeefConfig();
        Connection conn;
        Statement stat;

        String selectDistinctGroupQuery = new StringBuilder().append("SELECT DISTINCT attribute FROM ").append(NadeefConfiguration.getViolationTableName()).toString();

        try {
            conn  = DBConnectionPool.createConnection(dbConfig);
            stat = conn.createStatement();

            ResultSet rs = stat.executeQuery(selectDistinctGroupQuery);
            List<String> attributes = Lists.newArrayList();


            while (rs.next()) {
                String attribute = rs.getString("attribute");
                attributes.add(attribute);
            }
            rs.close();
            stat.close();
            conn.close();

            for(String attribute: attributes) {
                RepairGroup repairGroup = new RepairGroup(attribute, this.dirtyTableName, this.context);
                repairGroup.populateFix();
                this.repairGroups.add(repairGroup);
            }

            Collections.sort(repairGroups);

            for(int i=0;i<repairGroups.size();i++){
                if(!finishedGroups.contains(repairGroups.get(i).getName())){
                    finishedGroups.add(repairGroups.get(i).getName());
                    return repairGroups.get(i);
                }
            }
            return null;
        } catch (Exception e) {
            tracer.error("Cells could NOT be ordered");
            throw new NadeefDatabaseException(e);
        }


    }

}
