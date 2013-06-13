/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.io.Reader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Set;

/**
 * Nadeef configuration class.
 */
public class NadeefConfiguration {
    private static Tracer tracer = Tracer.getTracer(NadeefConfiguration.class);

    private static boolean testMode = false;
    private static DBConfig dbConfig;
    private static int maxIterationNumber = 1;
    private static boolean alwaysCompile = false;
    private static HashMap<String, RuleBuilder> ruleExtension = Maps.newHashMap();
    private static Optional<Class> decisionMakerClass;
    private static Path outputPath;
    //<editor-fold desc="Public methods">

    /**
     * Initialize configuration from string.
     * @param reader configuration string.
     */
    @SuppressWarnings("unchecked")
    public synchronized static void initialize(Reader reader) throws Exception {
        Preconditions.checkNotNull(reader);
        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);
        JSONObject database = (JSONObject)jsonObject.get("database");
        String url = (String)database.get("url");
        String userName = (String)database.get("username");
        String password = (String)database.get("password");
        String type;
        if (database.containsKey("type")) {
            type = (String)database.get("type");
        } else {
            type = "postgres";
        }

        DBConfig.Builder builder = new DBConfig.Builder();
        dbConfig =
            builder
                .url(url)
                .username(userName)
                .password(password)
                .dialect(CommonTools.getSQLDialect(type))
                .build();

        JSONObject general = (JSONObject)jsonObject.get("general");
        if (general.containsKey("testmode")) {
            testMode = (Boolean)general.get("testmode");
            Tracer.setVerbose(testMode);
        }

        if (general.containsKey("maxIterationNumber")) {
            maxIterationNumber = ((Long)general.get("maxIterationNumber")).intValue();
        }

        if (general.containsKey("alwaysCompile")) {
            alwaysCompile = (Boolean)(general.get("alwaysCompile"));
        }

        if (general.containsKey("fixdecisionmaker")) {
            String className = (String)general.get("fixdecisionmaker");
            Class customizedClass = CommonTools.loadClass(className);
            decisionMakerClass = Optional.of(customizedClass);
        } else {
            decisionMakerClass = Optional.absent();
        }

        if (general.containsKey("outputPath")) {
            String outputPathString = (String)general.get("outputPath");
            File tmpPath = new File(outputPathString);
            if (tmpPath.exists() && tmpPath.isDirectory()) {
                outputPath = tmpPath.toPath();
            } else {
                outputPathString = System.getProperty("user.dir");
                tracer.info(
                    "Cannot find directory " + outputPathString +
                    ", we change to working directory " + outputPathString
                );

                outputPath = new File(outputPathString).toPath();
            }
        }

        JSONObject ruleext = (JSONObject)jsonObject.get("ruleext");
        Set<String> keySet = (Set<String>)ruleext.keySet();
        for (String key : keySet) {
            String builderClassName = (String)ruleext.get(key);
            Class builderClass = CommonTools.loadClass(builderClassName);
            RuleBuilder writer = (RuleBuilder)(builderClass.getConstructor().newInstance());
            ruleExtension.put(key, writer);
        }
    }

    /**
     * Sets the test mode.
     * @param isTestMode test mode.
     */
    public static void setTestMode(boolean isTestMode) {
        testMode = isTestMode;
    }

    /**
     * Is Nadeef running in TestMode.
     * @return True when Nadeef is running in test mode.
     */
    public static boolean isTestMode() {
        return testMode;
    }

    /**
     * Gets the NADEEF output path. Output path is used for writing logs,
     * temporary class files, etc.
     * @return the NADEEF output path.
     */
    public static Path getOutputPath() {
        return outputPath;
    }

    /**
     * Gets the <code>DBConfig</code> of Nadeef metadata database.
     * @return meta data <code>DBConfig</code>.
     */
    public static DBConfig getDbConfig() {
        return dbConfig;
    }

    /**
     * Try gets the rule builder from the extensions.
     * @param typeName type name.
     * @return RuleBuilder instance.
     */
    public static RuleBuilder tryGetRuleBuilder(String typeName) {
        if (ruleExtension.containsKey(typeName)) {
            return ruleExtension.get(typeName);
        }
        return null;
    }

    /**
     * Gets the Nadeef installed schema name.
     * @return Nadeef DB schema name.
     */
    public static int getMaxIterationNumber() {
        return maxIterationNumber;
    }

    /**
     * Gets the Nadeef installed schema name.
     * @return Nadeef DB schema name.
     */
    public static String getSchemaName() {
        String schemaName = "public";
        return schemaName;
    }

    /**
     * Gets Nadeef violation table name.
     * @return violation table name.
     */
    public static String getViolationTableName() {
        return "violation";
    }

    /**
     * Gets Nadeef violation table name.
     * @return violation table name.
     */
    public static String getRepairTableName() {
        return "repair";
    }

    /**
     * Sets AlwaysCompile value.
     * @param alwaysCompile_ alwaysCompile value.
     */
    public static void setAlwaysCompile(boolean alwaysCompile_) {
        alwaysCompile = alwaysCompile_;
    }

    /**
     * Gets AlwaysCompile option.
     * @return alwaysCompile value.
     */
    public static boolean getAlwaysCompile() {
        return alwaysCompile;
    }

    /**
     * Gets the Nadeef version.
     * @return Nadeef version.
     */
    public static String getVersion() {
        String version = "Alpha";
        return version;
    }

    /**
     * Gets the Audit table name.
     * @return audit table name.
     */
    public static String getAuditTableName() {
        return "audit";
    }

    /**
     * Gets the decision maker class.
     * @return decision maker class. It is absent when user is not providing a customized
     */
    public static Optional<Class> getDecisionMakerClass() {
        return decisionMakerClass;
    }
    //</editor-fold>
}
