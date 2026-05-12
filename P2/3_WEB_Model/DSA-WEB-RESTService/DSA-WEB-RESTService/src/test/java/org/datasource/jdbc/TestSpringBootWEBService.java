package org.datasource.jdbc;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.logging.Logger;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSpringBootWEBService {
	private static Logger logger = Logger.getLogger(TestSpringBootWEBService.class.getName());

	private static String serviceURL = "http://localhost:8096/DSA-WEB-RESTService/rest/OLAP";
    private RestTemplate restTemplate = new RestTemplate();

	@Test
	public void test1_REST_OLAP_DIM_CUSTS_CITIES_DEPTS() {
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		headers.setBasicAuth("developer", "iis");
		String restDataEndpoint = serviceURL + "/OLAP_DIM_CUSTS_CITIES_DEPTS";
		logger.info(">>> test1_get_SalesView REST Data Endpoint: " + restDataEndpoint);
		ResponseEntity<String>  responseEntity = this.restTemplate.exchange(
				restDataEndpoint,
				HttpMethod.GET,
				new HttpEntity<>(null, headers),
				String.class
			);
		
		logger.info("ResultSet JSON (test 1): " + responseEntity.getBody());
	}
}