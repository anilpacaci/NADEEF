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

import qa.qcri.nadeef.core.pipeline.ExecutionContext;

/**
 * It is used to rank updates (VOI - F1/F2 etc).
 * Created by apacaci on 4/7/16.
 */
public class RankingManager {

    private ExecutionContext context;
    private String dirtyTableName;
    private String cleanTableName;

    public RankingManager(ExecutionContext context, String dirtyTableName, String cleanTableName) {
        this.context = context;
        this.dirtyTableName = dirtyTableName;
        this.cleanTableName = cleanTableName;
    }



    

}
