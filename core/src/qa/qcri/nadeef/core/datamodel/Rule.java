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

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import java.util.Collection;
import java.util.List;

/**
 * Abstract rule.
 */
public abstract class Rule<E> {
    protected String ruleName;
    protected List<String> tableNames;

    //<editor-fold desc="Constructor">
    /**
     * Default constructor.
     */
    protected Rule() {}

    /**
     * Constructor. Checks for which signatures are implemented.
     */
    protected Rule(String id, List<String> tableNames) {
        initialize(id, tableNames);
    }
    //</editor-fold>

    /**
     * Initialize a rule.
     * @param ruleName Rule id.
     * @param tableNames table names.
     */
    public void initialize(String ruleName, List<String> tableNames) {
        Preconditions.checkArgument(
            !Strings.isNullOrEmpty(ruleName) && tableNames != null && tableNames.size() > 0
        );

        this.ruleName = ruleName;
        this.tableNames = tableNames;
    }

    /**
     * Gets of rule name.
     */
    public String getRuleName() {
        return ruleName;
    }

    /**
     * Detect operator.
     * @param tuples input tuple.
     * @return Violation set.
     */
    public abstract Collection<Violation> detect(E tuples);

    /**
     * Repair operator.
     * @param violation violation input.
     * @return a candidate fix.
     */
    public abstract Collection<Fix> repair(Violation violation);

    /**
     * Block operator.
     * @param table input tuple
     * @return a generator of tuple collection.
     */
    public abstract Collection<Table> block(Collection<Table> table);

    /**
     * Iterator operator.
     * @param tables a collection of tables.
     * @param iteratorStream Iterator output object.
     */
    public abstract void iterator(
        Collection<Table> tables,
        IteratorStream<E> iteratorStream
    );

    /**
     * Vertical scope operator.
     * @param table input tables.
     * @return scoped tables.
     */
    public abstract Collection<Table> verticalScope(Collection<Table> table);

    /**
     * Horizontal scope operator.
     * @param table input tables.
     * @return scoped tables.
     */
    public abstract Collection<Table> horizontalScope(Collection<Table> table);

    /**
     * Returns <code>True</code> when the rule implements one tuple input.
     * @return <code>True</code> when the rule implements one tuple inputs.
     */
    public boolean supportOneInput() {
        return this instanceof SingleTupleRule;
    }

    /**
     * Returns <code>True</code> when the rule implements two tuple inputs.
     * @return <code>True</code> when the rule implements two tuple inputs.
     */
    public boolean supportTwoInputs() {
        return this instanceof PairTupleRule;
    }

    /**
     * Returns <code>True</code> when the rule implements multiple tuple inputs.
     * @return <code>True</code> when the rule implements multiple tuple inputs.
     */
    public boolean supportManyInputs() {
        return this instanceof SingleTupleRule;
    }

    /**
     * Returns <code>True</code> when the rule has two tables supported.
     * @return <code>True</code> when the rule has two tables supported.
     */
    public boolean supportTwoTables() {
        return tableNames.size() == 2;
    }

    /**
     * Gets the used table names in the rule.
     * @return A list of table names.
     */
    public List<String> getTableNames() {
        return tableNames;
    }
}
