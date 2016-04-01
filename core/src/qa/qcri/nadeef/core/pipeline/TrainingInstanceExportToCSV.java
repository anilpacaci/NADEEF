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

package qa.qcri.nadeef.core.pipeline;

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.TrainingInstance;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.core.utils.Violations;
import qa.qcri.nadeef.core.utils.sql.DBConnectionPool;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Logger;
import qa.qcri.nadeef.tools.PerfReport;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * Exports TrainingInstance list to a CSV file
 */
public class TrainingInstanceExportToCSV extends Operator<Collection<TrainingInstance>, File> {
    public TrainingInstanceExportToCSV(ExecutionContext context) {
        super(context);
    }

    @Override
    protected File execute(Collection<TrainingInstance> trainingInstances) throws Exception {
        Logger tracer = Logger.getLogger(TrainingInstanceExportToCSV.class);
        Path outputPath = NadeefConfiguration.getOutputPath();

        String filename =
            String.format("training_instances_%s_%d.csv",
                          getCurrentContext().getConnectionPool().getSourceDBConfig().getDatabaseName(),
                          System.currentTimeMillis()
            );

        File file = new File(outputPath.toFile(), filename);
        tracer.info("Export to " + file.getAbsolutePath());
        byte[] result = null;
        int size = 0;
        try (
            FileOutputStream fs = new FileOutputStream(file);
            BufferedOutputStream output = new BufferedOutputStream(fs);
        ) {
            for (TrainingInstance trainingInstance : trainingInstances) {
                StringBuffer line = new StringBuffer();


                Collection<Cell> cells = trainingInstance.getDirtyTuple().getCells();
                for (Cell cell : cells) {
                    if (cell.hasColumnName("tid"))
                        continue;
                    String value = cell.getValue() == null ? "" : cell.getValue().toString();
                    line
                        .append(
                            CommonTools.escapeString(
                                cell.getValue().toString(),
                                CommonTools.DOUBLE_QUOTE
                            )).append(",");

                }
                line.append(CommonTools.escapeString(
                    trainingInstance.getUpdatedCell().getValue().toString(),
                    CommonTools.DOUBLE_QUOTE
                )).append(",").
                    append(CommonTools.escapeString(
                        Double.toString(trainingInstance.getSimilarityScore()),
                    CommonTools.DOUBLE_QUOTE
                )).append(",").
                    append(CommonTools.escapeString(
                    trainingInstance.getLabel().toString(),
                    CommonTools.DOUBLE_QUOTE
                )).append("\n");
                byte[] bytes = line.toString().getBytes();
                output.write(bytes);

            }
            output.close();
            fs.close();
        }
        return file;
    }
}
