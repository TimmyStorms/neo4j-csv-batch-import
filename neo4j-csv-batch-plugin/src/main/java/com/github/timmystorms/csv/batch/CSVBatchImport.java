package com.github.timmystorms.csv.batch;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.Name;
import org.neo4j.server.plugins.Parameter;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

@Description("An extension to the Neo4j Server for batch importing CSV data")
public class CSVBatchImport extends ServerPlugin {

    @Name("csv_batch_import")
    @Description("Batch Import CSV file")
    @PluginTarget(GraphDatabaseService.class)
    public String getAllNodes(@Source final GraphDatabaseService graphDb,
            @Description("The CSV file path") @Parameter(name = "path", optional = false) final String filePath) {
        return new ExecutionEngine(graphDb).execute(
                "USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM '" + filePath
                        + "' AS csv MERGE (a{main:csv.val1,prop2:csv.val2}) MERGE (b{main:csv.val3})"
                        + " CREATE UNIQUE (a)-[r:relationshipname]->(b);").dumpToString();
    }

}
