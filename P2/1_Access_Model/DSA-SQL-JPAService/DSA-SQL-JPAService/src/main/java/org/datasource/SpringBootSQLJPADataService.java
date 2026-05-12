package org.datasource;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.logging.Logger;

/*
 * mvn spring-boot:run
 */
@SpringBootApplication
public class SpringBootSQLJPADataService
		extends SpringBootServletInitializer
{
	private static Logger logger = Logger.getLogger(SpringBootSQLJPADataService.class.getName());
	
	public static void main(String[] args) {
		logger.info("Loading ... SpringBootSQLJPADataService Default Settings ... JPA");
		SpringApplication.run(SpringBootSQLJPADataService.class, args);
	}
}

/*
JPQL Pagination
https://www.baeldung.com/jpa-pagination
 */