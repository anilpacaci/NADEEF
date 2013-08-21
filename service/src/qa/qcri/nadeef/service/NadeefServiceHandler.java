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

package qa.qcri.nadeef.service;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.thrift.TException;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.core.exception.InvalidRuleException;
import qa.qcri.nadeef.core.util.DBMetaDataTool;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.service.thrift.*;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * NadeefServiceHandler handles request for NADEEF service.
 */
// TODO: speedup the compiling stage by using object caching.
public class NadeefServiceHandler implements TNadeefService.Iface {
    private static Tracer tracer = Tracer.getTracer(NadeefServiceHandler.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public String generate(TRule tRule, String tableName) throws TNadeefRemoteException {
        String result = "";
        TRuleType type = tRule.getType();
        String code = tRule.getCode();
        String name = tRule.getName();

        switch (type) {
            case UDF:
                result = code;
                break;
            default:
                try {
                    Schema schema =
                        DBMetaDataTool.getSchema(
                            NadeefConfiguration.getDbConfig(), tableName
                        );
                    RuleBuilder ruleBuilder =
                        NadeefConfiguration.tryGetRuleBuilder(type.toString());
                    if (ruleBuilder != null) {
                        Collection<File> javaFiles =
                            ruleBuilder
                                .name(name)
                                .schema(schema)
                                .table(tableName)
                                .value(code)
                                .generate();
                        // TODO: currently only picks the first generated file
                        File codeFile = javaFiles.iterator().next();
                        result = Files.toString(codeFile, Charset.defaultCharset());
                    }
                } catch (Exception ex) {
                    tracer.err("Code generation failed.", ex);
                    TNadeefRemoteException re = new TNadeefRemoteException();
                    re.setType(TNadeefExceptionType.UNKNOWN);
                    re.setMessage(ex.getMessage());
                    throw re;
                }
                break;
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean verify(TRule rule) {
        TRuleType type = rule.getType();
        String code = rule.getCode();
        String name = rule.getName();
        boolean result = true;
        try {
            switch (type) {
                case UDF:
                    Path outputPath =
                        FileSystems.getDefault().getPath(
                            NadeefConfiguration.getOutputPath().toString(),
                            name + ".java"
                        );

                    Files.write(
                        code.getBytes(StandardCharsets.UTF_8),
                        outputPath.toFile()
                    );

                    if (!CommonTools.compileFile(outputPath.toFile())) {
                        result = false;
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception ex) {
            tracer.err("Exception happens in verify.", ex);
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String detect(TRule rule, String tableName) throws TNadeefRemoteException {
        if (!verify(rule)) {
            TNadeefRemoteException ex = new TNadeefRemoteException();
            ex.setType(TNadeefExceptionType.COMPILE_ERROR);
            throw ex;
        }

        try {
            NadeefJobScheduler scheduler = NadeefJobScheduler.getInstance();
            DBConfig config = new DBConfig(NadeefConfiguration.getDbConfig());
            TRuleType type = rule.getType();
            String name = rule.getName();
            String key = null;
            Rule ruleInstance;
            CleanPlan cleanPlan;
            switch (type) {
                case UDF:
                    Class udfClass = CommonTools.loadClass(name);
                    if (!Rule.class.isAssignableFrom(udfClass)) {
                        throw new InvalidRuleException("The specified class is not a Rule class.");
                    }

                    ruleInstance = (Rule) udfClass.newInstance();
                    ruleInstance.initialize(rule.getName(), Lists.newArrayList(tableName));
                    cleanPlan = new CleanPlan(config, ruleInstance);
                    key = scheduler.submitDetectJob(cleanPlan);
                    break;
                default:
                    Collection<Rule> rules = buildAbstractRule(rule, tableName);
                    for (Rule rule_ : rules) {
                        rule_.initialize(rule.getName(), Lists.newArrayList(tableName));
                        cleanPlan = new CleanPlan(config, rule_);
                        key = scheduler.submitDetectJob(cleanPlan);
                    }
                    break;
            }

            return key;

        } catch (InvalidRuleException ex) {
            tracer.err("Exception in detect", ex);
            TNadeefRemoteException re = new TNadeefRemoteException();
            re.setType(TNadeefExceptionType.INVALID_RULE);
            re.setMessage(ex.getMessage());
            throw re;
        } catch (Exception ex) {
            tracer.err("Exception in detect", ex);
            TNadeefRemoteException re = new TNadeefRemoteException();
            re.setType(TNadeefExceptionType.UNKNOWN);
            re.setMessage(ex.getMessage());
            throw re;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String repair(TRule rule, String tableName) throws TNadeefRemoteException {
        if (!verify(rule)) {
            TNadeefRemoteException ex = new TNadeefRemoteException();
            ex.setType(TNadeefExceptionType.COMPILE_ERROR);
            throw ex;
        }

        try {
            String name = rule.getName();
            Class udfClass = CommonTools.loadClass(name);
            if (!Rule.class.isAssignableFrom(udfClass)) {
                throw new InvalidRuleException("The specified class is not a Rule class.");
            }

            Rule ruleInstance = (Rule) udfClass.newInstance();
            ruleInstance.initialize(rule.getName(), Lists.newArrayList(tableName));
            DBConfig config = new DBConfig(NadeefConfiguration.getDbConfig());

            NadeefJobScheduler scheduler = NadeefJobScheduler.getInstance();
            String key = scheduler.submitRepairJob(new CleanPlan(config, ruleInstance));
            return key;
        } catch (InvalidRuleException ex) {
            tracer.err("Exception in detect", ex);
            TNadeefRemoteException re = new TNadeefRemoteException();
            re.setType(TNadeefExceptionType.INVALID_RULE);
            re.setMessage(ex.getMessage());
            throw re;
        } catch (Exception ex) {
            tracer.err("Exception in detect", ex);
            TNadeefRemoteException re = new TNadeefRemoteException();
            re.setType(TNadeefExceptionType.UNKNOWN);
            re.setMessage(ex.getMessage());
            throw re;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TJobStatus getJobStatus(String key) throws TException {
        NadeefJobScheduler jobScheduler = NadeefJobScheduler.getInstance();
        return jobScheduler.getJobStatus(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<TJobStatus> getAllJobStatus() throws TException {
        NadeefJobScheduler jobScheduler = NadeefJobScheduler.getInstance();
        return jobScheduler.getJobStatus();
    }

    private Collection<Rule> buildAbstractRule(TRule tRule, String tableName) throws Exception {
        TRuleType type = tRule.getType();
        String name = tRule.getName();
        String code = tRule.getCode();

        List<String> lines = Lists.newArrayList(code.split("\n"));

        RuleBuilder ruleBuilder = null;
        Collection<Rule> result = null;
        ruleBuilder = NadeefConfiguration.tryGetRuleBuilder(type.toString());
        Schema schema = DBMetaDataTool.getSchema(NadeefConfiguration.getDbConfig(), tableName);
        if (ruleBuilder != null) {
            result = ruleBuilder
                .name(name)
                .schema(schema)
                .table(tableName)
                .value(lines)
                .build();
        } else {
            tracer.err("Unknown Rule type: " + type, null);
            throw new IllegalArgumentException("Unknown rule type");
        }
        return result;
    }
}