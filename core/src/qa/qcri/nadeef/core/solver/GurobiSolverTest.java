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
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Operation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class GurobiSolverTest {
    public static void main(String[] args){
        // We are testing
        //      t.a > t.b, t.b > t.c
        //      t.a has value 2
        //      t.b has value 4
        //      t.c has value 6
        //      t.a > 3
        //      t.b != 5
        HashSet<Fix> fixSet = new HashSet<Fix>();
        Fix.Builder builder = new Fix.Builder();
        Cell ta = new Cell(new Column("T", "A"), 1, 1);
        Cell tb = new Cell(new Column("T", "B"), 1, 4.0);

//        fixSet.add(builder.left(ta).right(-1.0).op(Operation.LT).build());
//        fixSet.add(builder.left(ta).right(-3.0).op(Operation.LT).build());
//        fixSet.add(builder.left(ta).right(-4.0).op(Operation.LT).build());
//        fixSet.add(builder.left(ta).right(-5.6).op(Operation.LT).build());
//        fixSet.add(builder.left(ta).right(20.0).op(Operation.GT).build());
//        fixSet.add(builder.left(ta).right(2.0).op(Operation.LT).build());
//        fixSet.add(builder.left(ta).right(2.0).op(Operation.GT).build());
//        fixSet.add(builder.left(ta).right(1.0).op(Operation.EQ).build());
//        fixSet.add(builder.left(ta).right(1.0).op(Operation.EQ).build());
        fixSet.add(builder.left(ta).right(1).op(Operation.NEQ).build());
        fixSet.add(builder.left(ta).right(1).op(Operation.NEQ).build());
//        fixSet.add(builder.left(ta).right(1.0).op(Operation.GTE).build());
//        fixSet.add(builder.left(ta).right(1.0).op(Operation.GTE).build());

//        fixSet.add(builder.left(ta).right(13.1).op(Operation.GT).build());
//        fixSet.add(builder.left(tb).right(ta).op(Operation.GT).build());
        // fixSet.add(builder.left(tb).right(5).op(Operation.NEQ).build());
//        fixSet.add(builder.left(ta).right(tb).op(Operation.NEQ).build());
//        fixSet.add(builder.left(ta).right(tb).op(Operation.NEQ).build());
//        fixSet.add(builder.left(ta).right(tb).op(Operation.NEQ).build());
//        fixSet.add(builder.left(ta).right(tb).op(Operation.EQ).build());
//        fixSet.add(builder.left(ta).right(tb).op(Operation.EQ).build());
//        fixSet.add(builder.left(ta).right(tb).op(Operation.EQ).build());
//        fixSet.add(builder.left(ta).right(tb).op(Operation.EQ).build());
        Collection<Fix> result = new GurobiSolver().solve(fixSet,true);
        double va=0.0, vb=0.0;
        for (Fix fix : result) {
            if (fix.getLeft().equals(ta))
                va = Double.parseDouble(fix.getRightValue());
            if (fix.getLeft().equals(tb))
                vb = Double.parseDouble(fix.getRightValue());
        }
        System.out.println(va + " " + vb);
    }



}
