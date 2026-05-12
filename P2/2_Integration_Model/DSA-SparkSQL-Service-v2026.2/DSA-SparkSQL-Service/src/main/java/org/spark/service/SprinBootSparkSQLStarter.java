package org.spark.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.logging.Logger;


@SpringBootApplication(exclude={DataSourceAutoConfiguration.class} )
public class SprinBootSparkSQLStarter extends SpringBootServletInitializer
    {
        private static Logger logger = Logger.getLogger(SprinBootSparkSQLStarter.class.getName());

        public static void main(String[] args) throws Exception {
            logger.info("Loading ... SparkStarterService with Spark Default Settings ... DSA");
            SpringApplication.run(SprinBootSparkSQLStarter.class, args);
        }
}
// hive-exec:2.3.10 incompatibility with JDK21
// Fix: --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED

// java -jar target/DSA-SparkSQL-Service-2026.1.jar
// java --add-opens java.base/java.net=ALL-UNNAMED -jar target/DSA-SparkSQL-Service-2026.1.jar