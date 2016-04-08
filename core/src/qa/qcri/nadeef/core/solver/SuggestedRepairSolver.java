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

package qa.qcri.nadeef.core.solver;

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Fix;

import java.util.Collection;
import java.util.List;

/**
 * Created by y997zhan on 4/8/16.
 */
public class SuggestedRepairSolver {
    public List<Fix> solve(Collection<Fix> fixes){
        Cell cell=fixes.iterator().next().getLeft();
        Object originalValue=cell.getValue();
        if(originalValue instanceof String){
            return new VFMSolver().solve(fixes);
        }
        else if(originalValue instanceof Integer) return new GurobiSolver().solve(fixes,true);
        else return new GurobiSolver().solve(fixes,false);
    }
}
