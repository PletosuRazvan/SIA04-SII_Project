package org.spark.service.rest;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.spark.service.SparkSQLService;
import org.spark.service.exception.RESTSQLWorkflowException;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.logging.Logger;

@Service
public class SQLViewWorkflow {
    //
    private static Logger logger = Logger.getLogger(SQLViewWorkflow.class.getName());
    //
    private SparkSQLService sparkSQLService;
    //
    public SQLViewDefinition createJsonViewFromREST(String viewName, String restDataServiceHttpURL){
        if (viewName == null || viewName.isEmpty())
            throw new RuntimeException("SQLViewWorkflow: Call REST Endpoint Error: VIEW NAME is NULL or EMPTY");
        if (restDataServiceHttpURL == null || restDataServiceHttpURL.isEmpty())
            throw new RuntimeException("SQLViewWorkflow: Call REST Endpoint Error: VIEW HTTP.URL is NULL or EMPTY");
        //
        String jsonViewSchema = generateJsonSchema(restDataServiceHttpURL);
        return createJsonSQLView(viewName, jsonViewSchema, restDataServiceHttpURL);
    }

    // STEP_1_2
    public String generateJsonSchema(String restDataServiceHttpURL){
        if (restDataServiceHttpURL == null || restDataServiceHttpURL.isEmpty())
            throw new RuntimeException("SQLViewWorkflow: Call REST Endpoint Error: VIEW HTTP.URL is NULL or EMPTY!");
        //
        final SparkSession spark = sparkSQLService.getSpark();
        // 1. Call REST Endpoint
        String viewList;
        try {
            viewList = QueryRESTDataService.getRESTDataDocument(restDataServiceHttpURL);
            logger.info("DEBUG: REST Data: " + viewList);
        } catch (Exception e) {
            throw new RESTSQLWorkflowException("STEP_1: Call REST Endpoint Error for: ["
                    + restDataServiceHttpURL + "]\n"
                    + e.getMessage());
        }
        // 2.Get JSON schema from REST Endpoint
        String jsonViewSchema;
        // Escape single quotes in JSON data to avoid SQL parsing errors
        // In SparkSQL, single quotes inside string literals are escaped by doubling them
        String escapedViewList = viewList.replace("'", "''");
        String jsonSchemaQuery = String.format("""
                        SELECT schema_of_json('%s') as json_schema
                    """, escapedViewList);
        try {
            logger.info("DEBUG: jsonSchemaQuery: " + jsonSchemaQuery);
            List<Row> jsonViewSchemaDF = spark.sql(jsonSchemaQuery).collectAsList();
            logger.info("DEBUG: JSON Schema DF: " + jsonViewSchemaDF);
            jsonViewSchema = jsonViewSchemaDF
                    .get(0).getAs("json_schema").toString();
            logger.info("DEBUG: JSON Schema: " + jsonViewSchema);
        } catch (Exception e) {
            throw new RESTSQLWorkflowException("STEP_2: Get JSON schema from REST Endpoint Error for: ["
                    + jsonSchemaQuery + "]\n"
                    + e.getMessage());
        }
        //
        return jsonViewSchema;
    }

    // Split creation process in 2 steps: JSONViewn and SQLView
    public SQLViewDefinition createJsonSQLView(String viewName, String jsonViewSchema, String restDataServiceHttpURL){
        // 3. Create View
        String CREATE_VIEW_Query = String.format("""
                CREATE OR REPLACE VIEW %1$s AS
                    SELECT from_json(
                            json_raw.data,
                            '%2$s'
                        ) array
                    FROM (SELECT java_method(
                            'org.spark.service.rest.QueryRESTDataService', 'getRESTDataDocument',
                            '%3$s'
                           )as data) json_raw
            """, viewName, jsonViewSchema, restDataServiceHttpURL);
        try{
            logger.info("DEBUG: createJsonSQLView: " + CREATE_VIEW_Query);
            //
            sparkSQLService.getSpark().sql(CREATE_VIEW_Query);
        }catch (Exception e) {
            throw new RESTSQLWorkflowException("STEP_3: CREATE VIEW error for: ["
                    + CREATE_VIEW_Query + "]\n"
                    + e.getMessage());
        }

        // 4. Check view created
        String checkViewQuery = String.format("SELECT * FROM %s LIMIT 1", viewName);
        try {
            List<Row> checkList = sparkSQLService.getSpark()
                    .sql(checkViewQuery)
                    .collectAsList();
            if (checkList == null)
                throw new RuntimeException("View creation failed!!!");
        }catch (Exception e) {
            throw new RESTSQLWorkflowException("STEP_4: CHECK VIEW error: ["
                    + checkViewQuery + "]\n"
                    + e.getMessage());
        }
        // 5. Return view descriptor if created
        return new SQLViewDefinition(viewName, restDataServiceHttpURL, jsonViewSchema,
                CREATE_VIEW_Query, "/rest/view/" + viewName);
    }

    // execute Query from SQL-REST.API: RESTEnabledSQLService
    public SQLResponse executeSQLQuery(String SQLQuery){
        if (SQLQuery == null || SQLQuery.isEmpty())
            throw new RuntimeException("STEP_0: Call REST Endpoint Error: query is NULL or EMPTY");
        //
        logger.info("DEBUG: Start execute: " + SQLQuery);
        //
        final SparkSession spark = sparkSQLService.getSpark();
        String sqlResults;
        try {
            Dataset<Row> sqlDataSet = spark.sql(SQLQuery);
            logger.info("DEBUG: query result: ");
            sqlDataSet.show();
            sqlResults = sqlDataSet.toJSON().collectAsList().toString();
            logger.info("DEBUG: query result string:\n " + sqlResults + "\n");
        } catch (Exception e) {
            throw new RESTSQLWorkflowException("STEP_1: SQL Execution Error for: ["
                    + SQLQuery + "]\n"
                    + e.getMessage());
        }
        //
        return new SQLResponse(SQLQuery, sqlResults);
    }

    // Auto Autowired
    public SQLViewWorkflow(SparkSQLService sparkSQLService) {
        this.sparkSQLService = sparkSQLService;
    }

}
