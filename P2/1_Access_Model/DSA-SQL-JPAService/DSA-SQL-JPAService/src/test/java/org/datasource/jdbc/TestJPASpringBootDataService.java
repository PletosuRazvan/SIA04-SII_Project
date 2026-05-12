package org.datasource.jdbc;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.util.logging.Logger;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestJPASpringBootDataService {
	private static Logger logger = Logger.getLogger(TestJPASpringBootDataService.class.getName());

	private static String serviceURL = "http://localhost:8091/DSA_SQL_JPAService/rest/sales";
    private RestTemplate restTemplate = new RestTemplate();

	@Test
	public void test1_get_SalesView() {
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		headers.setBasicAuth("developer", "iis");
		String restDataEndpoint = serviceURL + "/SalesView";
		logger.info(">>> test1_get_SalesView REST Data Endpoint: " + restDataEndpoint);
		ResponseEntity<String>  responseEntity = this.restTemplate.exchange(
				restDataEndpoint,
				HttpMethod.GET,
				new HttpEntity<>(null, headers),
				String.class
			);
		
		logger.info("ResultSet JSON (test 1): " + responseEntity.getBody());
	}

	@Test
	public void test2_InvoicesSalesView() {
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		headers.setBasicAuth("developer", "iis");
		String restDataEndpoint = serviceURL + "/InvoicesSalesView";
		logger.info(">>> test2_InvoicesSalesView REST Data Endpoint: " + restDataEndpoint);
		ResponseEntity<String>  responseEntity = this.restTemplate.exchange(
				restDataEndpoint,
				HttpMethod.GET,
				new HttpEntity<>(null, headers),
				String.class
		);

		logger.info("ResultSet JSON (test 2): " + responseEntity.getBody());
	}

	//@Test
	public void test2_InvoicesSalesView_User() {
		HttpHeaders headers = new HttpHeaders();
		headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
		//headers.setBasicAuth("developer", "iis");
		String restDataEndpoint = "http://developer:iis@localhost:8091/DSA_SQL_JPAService/rest/sales/InvoicesSalesView";
		logger.info(">>> test2_InvoicesSalesView REST Data Endpoint: " + restDataEndpoint);
		ResponseEntity<String>  responseEntity = this.restTemplate.exchange(
				restDataEndpoint,
				HttpMethod.GET,
				new HttpEntity<>(null, headers),
				String.class
		);

		logger.info("ResultSet JSON (test 2): " + responseEntity.getBody());
	}
}