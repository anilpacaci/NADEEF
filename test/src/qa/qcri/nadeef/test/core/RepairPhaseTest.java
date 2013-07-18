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

package qa.qcri.nadeef.test.core;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.Tracer;

/**
 * Test the repair phase of NADEEF.
 */
public class RepairPhaseTest {
    private NodeCacheManager cacheManager;

    @Before
    public void setup() {
        Bootstrap.start();
        Tracer.setVerbose(true);
        Tracer.setInfo(true);
        cacheManager = NodeCacheManager.getInstance();
    }

    @After
    public void teardown() {
        Assert.assertEquals(2, cacheManager.getSize());
        cacheManager.clear();
    }

    @Test
    public void test1() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan3();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            Integer count = (Integer)executor.detect().getDetectOutput();
            Assert.assertEquals(1, count.intValue());

            count = (Integer)executor.repair().getRepairOutput();
            Assert.assertEquals(1, count.intValue());

            count = (Integer)executor.getUpdateOutput();
            Assert.assertEquals(1, count.intValue());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("exceptions : " + ex.getMessage());
        }
    }

    @Test
    public void test2() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            Integer count = (Integer)executor.detect().getDetectOutput();
            Assert.assertEquals(2, count.intValue());

            count = (Integer)executor.repair().getRepairOutput();
            Assert.assertEquals(4, count.intValue());

            count = (Integer)executor.getUpdateOutput();
            Assert.assertEquals(2, count.intValue());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("exceptions : " + ex.getMessage());
        }
    }
}
