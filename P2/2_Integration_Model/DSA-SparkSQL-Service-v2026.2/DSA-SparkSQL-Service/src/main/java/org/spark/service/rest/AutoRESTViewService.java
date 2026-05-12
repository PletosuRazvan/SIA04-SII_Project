package org.spark.service.rest;

import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.spark.service.SparkSQLService;
import org.spark.service.exception.RESTSQLWorkflowException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/*	REST Service URL:
	http://localhost:9990/DSA-SparkSQL-Service/rest/view/{VIEW_NAME}
	http://localhost:9990/DSA-SparkSQL-Service/rest/STRUCT/{VIEW_NAME}
	http://localhost:9990/DSA-SparkSQL-Service/rest/ping
 */
@RestController @RequestMapping("/rest")
public class AutoRESTViewService {
    private static Logger logger = Logger.getLogger(AutoRESTViewService.class.getName());
    //
    @Value( "${sparksql.autorest.mode}")
    private String sparksqlAutorestMode;
    //
    private final SparkSQLService sparkSQLService;
    // Auto-Autowired
    public AutoRESTViewService(SparkSQLService sparkSQLService) {
        this.sparkSQLService = sparkSQLService;
        //
    }
    @PostConstruct
    private void init(){
        if ("on".equals(sparksqlAutorestMode.toLowerCase().trim())
                || "restricted".equals(sparksqlAutorestMode.toLowerCase().trim())) {
            loadViewDefs();
            this.sparksqlAutorestMode = sparksqlAutorestMode.toLowerCase().trim();
            logger.info("DEBUG: AutoRESTViewService: viewMap: " + viewMap);
        }
        else
            this.sparksqlAutorestMode = "off";
        logger.info("DEBUG: views AUTORESTing policy: " + sparksqlAutorestMode);
    }
    //
    @RequestMapping(value = "/ping", method = RequestMethod.GET,
            produces = {MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public String pingDataSource() {
        if ("off".equals(sparksqlAutorestMode))
            throw new RESTSQLWorkflowException("SparkSQL AUTO REST Service is not available!");
        //
        return "PING response from SparkSQLRESTService!";
    }
    //
    @RequestMapping(value = "/view/**", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public String get_ViewDataSet(HttpServletRequest request,
                                  @RequestParam(required = false) boolean redef) throws Exception {
        if ("off".equals(sparksqlAutorestMode))
            throw new RESTSQLWorkflowException("SparkSQL AUTO REST Service is not available (off)!");
        if (redef) loadViewDefs();

        String base = "/rest/view/";
        String viewRESTPath = request.getRequestURI()
                .substring(request.getRequestURI().indexOf(base) + base.length());
        logger.info("DEBUG: get_ViewDataSet: Querying View REST PATH: " + viewRESTPath);
        //
        String viewName = getViewName(viewRESTPath);
        if (viewName == null)
            throw new RESTSQLWorkflowException("REST Error: viewName for " + viewRESTPath + " is NULL!");
        //
        Dataset<Row> viewDataSet =  this.sparkSQLService.getSpark().sql("SELECT * FROM " + viewName);
        // DEBUG: View Data Set
        logger.info("DEBUG: get_ViewDataSet: View Schema: ");
        viewDataSet.printSchema();
        //viewDataSet.show();
        //
        String jsonList = viewDataSet.toJSON().collectAsList().toString();
        return jsonList;
    }

    // If VIEW definitions by ALTER VIEW TBLPROPERTIES('AUTOREST') are changed, then reload them
    @RequestMapping(value = "/auto", method = RequestMethod.GET,
            produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.TEXT_PLAIN_VALUE})
    @ResponseBody
    public Map<String, String> action_ReloadViewRestDef(@RequestParam boolean redef) throws Exception {
        if ("off".equals(sparksqlAutorestMode))
            throw new RESTSQLWorkflowException("SparkSQL AUTO REST Service is not available (off)!");
        if (redef) loadViewDefs();

        return this.viewMap;
    }

    //
    @RequestMapping(value = "/STRUCT/**", method = RequestMethod.GET,
            produces = {MediaType.TEXT_PLAIN_VALUE, MediaType.APPLICATION_JSON_VALUE})
    @ResponseBody
    public String get_ViewDataSTRUCT(HttpServletRequest request,
                                     @RequestParam(required = false) boolean redef) throws Exception {
        if ("off".equals(sparksqlAutorestMode))
            throw new RESTSQLWorkflowException("SparkSQL AUTO REST Service is not available!");
        if (redef) loadViewDefs();
        //
        String base = "/rest/STRUCT/";
        String viewRESTPath = request.getRequestURI()
                .substring(request.getRequestURI().indexOf(base) + base.length());
        //
        String viewName = getViewName(viewRESTPath);
        logger.info("DEBUG: get_ViewDataSTRUCT: Querying View REST named: " + viewName);
        if (viewName == null)
            throw new RESTSQLWorkflowException("REST Error: viewName for " + viewRESTPath + " is NULL!");
        //
        Dataset<Row> viewDataSet =  this.sparkSQLService.getSpark().sql(
                "SELECT * FROM " + viewName + " WHERE 1=0");
        // DEBUG: View Data Set
        logger.info("DEBUG: get_ViewDataSTRUCT: View Schema: ");
        viewDataSet.printSchema();
        //
        String viewSchema = viewDataSet.schema().sql();
        return viewSchema;
    }

    // Resolve VIEW name from URL
    // ALTER VIEW <viewName> SET TBLPROPERTIES('AUTOREST' = "/module-path/<viewName>");
    // ALTER VIEW OLAP_DIM_CUSTS_CITIES_DEPTS SET TBLPROPERTIES('AUTOREST' = "olap/dim/custs_cities_depts");
    private String getViewName(String viewURL) {
        if ("off".equals(sparksqlAutorestMode))
            throw new RESTSQLWorkflowException("SparkSQL AUTO REST Service is not available!" +
                    " Check: sparksql.autorest.enabled property!");

        // Process VIEWs metadata from viewMap
        logger.info("DEBUG: views AUTORESTing searching: " + viewURL);
        for(String viewName: viewMap.keySet()) {
            //
            if (viewURL.toUpperCase().equals(viewName) ||
                    viewURL.toUpperCase().equals(viewMap.get(viewName).toUpperCase())) {
                //
                logger.info("DEBUG: views AUTORESTing: " + viewName + " -> " + viewMap.get(viewName));
                switch (sparksqlAutorestMode) {
                    case "on" -> {return viewName;}
                    case "restricted" -> {
                        if (viewMap.get(viewName).equals("NONE"))
                            throw new RESTSQLWorkflowException("REST Error: " + viewURL +
                                    " VIEW is not REST enabled!");
                        return viewName;
                    }
                    case "off" ->
                            throw new RESTSQLWorkflowException("REST Error: AutoREST disabled, " +
                                    "sparksql.autorest.mode is off!");
                    default -> throw new RESTSQLWorkflowException("REST Error: AutoREST disabled, " +
                            "sparksql.autorest.mode is invalid:" + sparksqlAutorestMode + "!");
                }
            }
        }
        // not registered view
        return null;
    }

    private Map<String, String> viewMap = new HashMap<>();
    private void loadViewDefs(){
        // Get VIEWs metadata into viewMap
        List<Row> viewDataSet =  this.sparkSQLService.getSpark().sql("SHOW VIEWS").collectAsList();
        viewMap = new HashMap<>();
        for (Row row: viewDataSet){
            String showViewCommand = String.format("SHOW TBLPROPERTIES %s('AUTOREST')", row.getString(1));
            String autorest = sparkSQLService.getSpark().sql(showViewCommand).first().getString(1);
            if (autorest == null || autorest.isBlank() || autorest.isEmpty()
                    || autorest.contains("does not have property: AUTOREST"))
                autorest = "NONE";
            viewMap.put(row.getString(1).toUpperCase(), autorest.toUpperCase());
        }
    }
}