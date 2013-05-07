/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 */
public abstract class PairTupleRule extends Rule<TuplePair, Collection<TuplePair>> {
    public PairTupleRule() {
        super();
    }

    public PairTupleRule(String id, List<String> tableNames) {
        super(id, tableNames);
    }

    /**
     * Detect rule with pair tuple.
     *
     * @param pair input tuple pair.
     * @return Violation set.
     */
    @Override
    public abstract Collection<Violation> detect(TuplePair pair);

    /**
     * Default group operation.
     *
     * @param tupleCollections input tuple
     * @return a group of tuple collection.
     */
    @Override
    public Collection<TuplePair> group(Collection<TupleCollection> tupleCollections) {
        ArrayList<TuplePair> result = new ArrayList();
        List<TupleCollection> collectionList = Lists.newArrayList(tupleCollections);

        if (collectionList.size() == 1) {
            TupleCollection tuples = collectionList.get(0);
            for (int i = 0; i < tuples.size(); i ++) {
                for (int j = i + 1; j < tuples.size(); j ++) {
                    TuplePair pair = new TuplePair(tuples.get(i), tuples.get(j));
                    result.add(pair);
                }
            }
        } else {
            TupleCollection left = collectionList.get(0);
            TupleCollection right = collectionList.get(0);
            for (int i = 0; i < left.size(); i ++) {
                for (int j = i + 1; j < right.size(); j ++) {
                    TuplePair pair = new TuplePair(left.get(i), right.get(j));
                    result.add(pair);
                }
            }

        }
        return result;
    }

    /**
     * Default scope operation.
     *
     * @param tupleCollections input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public Collection<TupleCollection> scope(Collection<TupleCollection> tupleCollections) {
        return tupleCollections;
    }
}