package org.spark.service;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2;
import org.springframework.stereotype.Service;

import java.util.logging.Logger;

@Service
public class SparkSQLService {
    private static Logger logger = Logger.getLogger(SparkSQLService.class.getName());

    //
    private SparkSession spark;

    public SparkSession getSpark() {
        return spark;
    }

    public SparkSQLService() {
        startThriftServer2();
    }
    /*
    * https://spark.apache.org/docs/latest/sql-distributed-sql-engine.html
    *
     */
    private void startThriftServer2(){
        logger.info(">>> HiveThriftServer2 Starting ....");
        // Create a SparkSession with Hive support
        this.spark = SparkSession.builder()
                .master("local[*]").config("spark.ui.port", "8081")
                .appName("SparkSQL-REST.Server")
                .enableHiveSupport()
                .config("hive.server2.thrift.port", "10000")
                .getOrCreate();

        // Start the Thrift server
        HiveThriftServer2.startWithContext(spark.sqlContext());
        logger.info(">>> HiveThriftServer2 started successfully!");
    }
}

// Check WebUI: http://localhost:8081/
// java -jar target/DSA-SparkSQL-Service-2026.1.jar
//                .config("spark.sql.warehouse.dir", "file:////home/catalin-strimbei/Professionals/IIS/apps/IIS_DSA/spark-warehouse")
//                .config("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=/home/catalin-strimbei/Professionals/IIS/apps/IIS_DSA/metastore_db;create=true")
