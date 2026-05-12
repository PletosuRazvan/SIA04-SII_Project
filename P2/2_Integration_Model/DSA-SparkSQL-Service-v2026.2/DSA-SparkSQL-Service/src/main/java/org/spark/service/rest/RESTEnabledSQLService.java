package org.spark.service.rest;

import org.spark.service.exception.RESTSQLWorkflowException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.logging.Logger;

/* REST Enabled SQL API:
    http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/query
    http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/create-json-view-from-rest?view_name={view_name}
 */

@RestController
@RequestMapping("/_sqlrest")
public class RESTEnabledSQLService {
    //
    private static Logger logger = Logger.getLogger(RESTEnabledSQLService.class.getName());
    //
    @Value("${sparksql.rest.enabled}")
    private Boolean sparkRestEnabled = false;
    //
    private SQLViewWorkflow sqlViewWorkflow;
    //
    @PostMapping(value = "/query",
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public SQLResponse executePostQuery(@RequestBody String SQLQuery){
        if (!sparkRestEnabled)
            throw new RESTSQLWorkflowException("SparkSQL REST Enabled Service is not available!");

        if (SQLQuery == null || SQLQuery.isEmpty())
            throw new RuntimeException("STEP_0: Call REST Endpoint Error: query is NULL or EMPTY");
        logger.info("DEBUG: executePostQuery Start execute: " + SQLQuery);
        //
        return sqlViewWorkflow.executeSQLQuery(SQLQuery);
    }
    //
    @GetMapping(value = "/query",
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public SQLResponse executeGetQuery(@RequestParam("sql") String SQLQuery){
        if (!sparkRestEnabled)
            throw new RESTSQLWorkflowException("SparkSQL REST Enabled Service is not available!");

        if (SQLQuery == null || SQLQuery.isEmpty())
            throw new RuntimeException("STEP_0: Call REST Endpoint Error: query is NULL or EMPTY");
        logger.info("DEBUG: executeGetQuery Start execute: " + SQLQuery);
        //
        return sqlViewWorkflow.executeSQLQuery(SQLQuery);
    }

    //
    /* SQLViewWorkflow Processing Logic:
    * CREATE SQL JSON VIEW from JSON Resource with REST Endpoint URL
    * */
    @PostMapping(value = "/create-json-view-from-rest",
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public SQLViewDefinition createJsonViewFromREST(@RequestParam("view_name") String viewName,
                                                    @RequestBody String restDataServiceHttpURL){
        if (!sparkRestEnabled)
            throw new RESTSQLWorkflowException("SparkSQL REST Enabled Service is not available!");

        if (viewName == null || viewName.isEmpty())
            throw new RuntimeException("STEP_0: Call REST Endpoint Error: VIEW NAME is NULL or EMPTY");
        if (restDataServiceHttpURL == null || restDataServiceHttpURL.isEmpty())
            throw new RuntimeException("STEP_0: Call REST Endpoint Error: VIEW HTTP.URL is NULL or EMPTY");
        //
        logger.info("DEBUG: Start CREATE: " + viewName + " from: " + restDataServiceHttpURL);
        //
        return sqlViewWorkflow.createJsonViewFromREST(viewName, restDataServiceHttpURL);
    }

    /* To call REST-based createJSONView() from SparkSQL clients using java_method() function:
    SELECT java_method(
               'org.spark.service.rest.RESTEnabledSQLService',
               'createJSONViewFromREST',
               'CUSTOMERS_JSON_VIEW',
               'http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView');
     */
    public static String createJSONViewFromREST(String viewName, String restDataServiceHttpURL){
        if (serverPort == null || serverPort.isEmpty() ||
                serverServletContextPath == null || serverServletContextPath.isEmpty())
            throw new RESTSQLWorkflowException("REST INITIALIZATION Error: check server.port " +
                    "and server.servlet.context-path !");

        if (viewName == null || viewName.isEmpty())
            throw new RuntimeException("REST Error: viewName is NULL or EMPTY");

        if (restDataServiceHttpURL == null || restDataServiceHttpURL.isEmpty())
            throw new RuntimeException("REST Error: restDataServiceHttpURL is NULL or EMPTY");

        // 1. Call REST Endpoint
        String restData = null;
        try {
            RestTemplate restTemplate = new RestTemplate();
            //
            HttpHeaders headers = new HttpHeaders();
            headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
            // Put json-view-name in the URL
            String restDataEndpoint = String.format(
                            "http://localhost:%1$s%2$s/_sqlrest/create-json-view-from-rest?view_name=%3$s",
                            serverPort, serverServletContextPath, viewName);
            logger.info("DEBUG: create JSON View From REST endpoint: " + restDataEndpoint);
            //
            ResponseEntity<String> responseEntity = restTemplate.exchange(
                    restDataEndpoint,
                    HttpMethod.POST,
                    // Put REST Endpoint URL in the body
                    new HttpEntity<>(restDataServiceHttpURL, headers),
                    String.class
            );
            restData = responseEntity.getBody();
            logger.info("DEBUG: generateJSONView REST Data: " + restData);
        } catch (Exception e) {
            throw new RESTSQLWorkflowException("REST Error: " + e.getMessage());
        }
        return restData;
    }

    // Auto Autowired
    public RESTEnabledSQLService(SQLViewWorkflow workflow) {
        this.sqlViewWorkflow = workflow;
    }
    //
    private static String serverPort;
    private static String serverServletContextPath;
    //
    @Value("${server.port}")
    public void setServerPort(String port) {
        serverPort = port;
    }
    @Value("${server.servlet.context-path}")
    public void setServerServletContextPath(String path){
        serverServletContextPath = path;
    }
}

/*
 ## CREATE VIEW Query
 curl -i -X POST \
  --user spark:sql \
  --data "http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView" \
  -H "Content-Type: text/plain" \
  -k http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/create-json-view-from-rest?view_name=CUSTOMER_VIEW_DATA

curl -i -X POST --user spark:sql --data "http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView" -H "Content-Type: text/plain"  -k http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/create-json-view-from-rest?view_name=CUSTOMER_VIEW_DATA

## REST SQL
curl -i -X POST \
  --user spark:sql \
  --data "SELECT * FROM CUSTOMERS_VIEW" \
  -H "Content-Type: text/plain" \
  -k http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/query

curl -i -X POST \
  --user spark:sql \
  --data "SELECT * FROM OLAP_VIEW_SALES_DEP_CIT_CUST" \
  -H "Content-Type: text/plain" \
  -k http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/query

curl -i -X GET --user spark:sql\
 "http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/query"\
 --url-query "sql=SELECT * FROM CUSTOMERS_VIEW"

curl -G --user spark:sql\
 "http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/query"\
 --url-query "sql=SELECT * FROM CUSTOMERS_VIEW"

curl -G --user spark:sql\
 "http://localhost:9990/DSA-SparkSQL-Service/_sqlrest/query"\
 --data-urlencode "sql=SELECT * FROM CUSTOMERS_VIEW"
*/