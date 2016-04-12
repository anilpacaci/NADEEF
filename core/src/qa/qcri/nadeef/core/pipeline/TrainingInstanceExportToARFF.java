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

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.exceptions.NadeefDatabaseException;
import qa.qcri.nadeef.core.utils.sql.ValueHelper;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Exports TrainingInstance list to a CSV file
 */
public class TrainingInstanceExportToARFF extends Operator<Collection<TrainingInstance>, Integer> {

    private static Logger tracer = Logger.getLogger(TrainingInstanceExportToARFF.class);

    public TrainingInstanceExportToARFF(ExecutionContext context) {
        super(context);
    }

    @Override
    protected Integer execute(Collection<TrainingInstance> trainingInstances) throws Exception {

        Map<String, List<TrainingInstance>> trainingInstancesClusters = Maps.newHashMap();

        //we need to cluster training instances by attribute
        for(TrainingInstance instance : trainingInstances) {
            String attributeName = instance.getAttribute();
            if(trainingInstancesClusters.containsKey(attributeName)) {
                trainingInstancesClusters.get(attributeName).add(instance);
            } else {
                List<TrainingInstance> newListForAttribute = Lists.newArrayList();
                newListForAttribute.add(instance);
                trainingInstancesClusters.put(attributeName, newListForAttribute);
            }
        }


        for(String clusteringAttribute : trainingInstancesClusters.keySet()) {
            List<TrainingInstance> clusterInstances = trainingInstancesClusters.get(clusteringAttribute);
            writeClusterToFile(clusteringAttribute, clusterInstances);
        }

        return trainingInstancesClusters.size();
    }

    private String generateAttributeString(Schema databaseSchema, Column column, String tableName) throws SQLException, NadeefDatabaseException {
        DataType type = databaseSchema.getType(column.getColumnName());
        String attributeLine = null;
        // numeric is the easy part
        if(type.equals(DataType.DOUBLE) || type.equals(DataType.FLOAT) || type.equals(DataType.INTEGER)) {
            attributeLine = new StringBuilder().append("@attribute ").append(column.getColumnName()).append(" numeric\n").toString();
        } else if(type.equals(DataType.BOOL) || type.equals(DataType.STRING)) {
            // retrieve distinct values from database, create nominal attribute
            List<String> distinctValues = ValueHelper.getInstance().getDistinctValues(tableName, column.getColumnName());
            StringBuilder attributeLineBuilder = new StringBuilder().append("@attribute ").append(column.getColumnName()).append(" {");
            String distinctValuesString = distinctValues.stream().map((s) -> "\"" + s + "\"").collect(Collectors.joining(","));
            attributeLineBuilder.append(distinctValuesString).append(" }\n");
            attributeLine = attributeLineBuilder.toString();
        }
        return  attributeLine;
    }

    private void writeClusterToFile(String attributeName, List<TrainingInstance> trainingInstances) throws  Exception{
        //TODO: assume that training instances are not empty
        if(trainingInstances.isEmpty()) {
            // no training instance, so we cannot generate any
            return;
        }
        TrainingInstance sampleInstance = trainingInstances.iterator().next();
        Schema databaseSchema = sampleInstance.getDirtyTuple().getSchema();
        Cell dirtyCell = sampleInstance.getDirtyTuple().getCell(sampleInstance.getAttribute());
        String updatedValue = sampleInstance.getUpdatedValue();
        String tableName = databaseSchema.getTableName();

        List<String> permittedAttributes = NadeefConfiguration.getMLAttributes();

        Path outputPath = NadeefConfiguration.getOutputPath();
        String filename =
            String.format("training_instances_%s_%d.arff",
                          attributeName,
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
            // handle @relation part
            String relationLine = new StringBuffer().append("@relation ").append(attributeName).append("\n").toString();
            output.write(relationLine.getBytes());

            // handle the attribute definitions of tuple, it might be tricky cause it requires to handle
            for(Column column : databaseSchema.getColumns()) {
                if(!permittedAttributes.contains(column.getColumnName().toLowerCase())) {
                    continue;
                }
                String attributeLine = generateAttributeString(databaseSchema, column, tableName);
                output.write(attributeLine.getBytes());
            }

            // finally handle additional attributes, new value, similarity and label
            // TODO YOU have to generate seperate file for each attbiute so taht you know which column you are working on
            String newValueLine = generateAttributeString(databaseSchema, dirtyCell.getColumn(), tableName);
            newValueLine.replace("STATE", "new_value");
            output.write(newValueLine.getBytes());
            String scoreLine = "@attribute similarity_score numeric\n";
            output.write(scoreLine.getBytes());
            String classLabelLine = "@attribute class_label {YES, NO}\n";
            output.write(classLabelLine.getBytes());


            String dataHeaderLine = new String("@data\n");
            output.write(dataHeaderLine.getBytes());

            //now ready to create data part
            for (TrainingInstance trainingInstance : trainingInstances) {
                StringBuffer line = new StringBuffer();


                Collection<Cell> cells = trainingInstance.getDirtyTuple().getCells();
                for (Cell cell : cells) {
                    if (!permittedAttributes.contains(cell.getColumn().getColumnName().toLowerCase()))
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
                    trainingInstance.getUpdatedValue(),
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

    }
}
