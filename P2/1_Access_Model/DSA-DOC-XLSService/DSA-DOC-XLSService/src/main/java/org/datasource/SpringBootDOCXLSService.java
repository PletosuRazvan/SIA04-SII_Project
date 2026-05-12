package org.datasource;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.logging.Logger;


/*
 * java -jar DSA-DOC-XLSService-2025.1.jar --xlsx.data.source.file.path=/home/catalin/data/CustProdCateg.xlsx
 */
@SpringBootApplication (exclude={DataSourceAutoConfiguration.class} )
public class SpringBootDOCXLSService extends SpringBootServletInitializer {
	private static Logger logger = Logger.getLogger(SpringBootDOCXLSService.class.getName());
	
	public static void main(String[] args) {
		logger.info("Loading ... SpringBootDOCXLSService Default Settings ... XLSx");
		SpringApplication.run(SpringBootDOCXLSService.class, args);
	}
}