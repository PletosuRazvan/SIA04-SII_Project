package org.spark.service.rest;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Logger;

/*
SELECT java_method('org.spark.service.rest.QueryRESTDataService', 'pingService')
 */
public class QueryRESTDataService {
    private static Logger logger = Logger.getLogger(QueryRESTDataService.class.getName());

    public static String pingService(){
        return "Pinged Spark SQL Thrift Server!";
    }

    public static String getRESTDataDocument(String httpURL){
        RestTemplate restTemplate = new RestTemplate();
        //
        logger.info("DEBUG: getRESTDataDocument ...");
        HttpHeaders headers = new HttpHeaders();
        headers.add(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE);
        // Manage credentials
        String[] credentials = parseCredentials(httpURL);
        if (credentials != null) {
            String username = credentials[0];
            String password = credentials[1];
            if (username != null && password != null)
            headers.setBasicAuth(username, password);
            httpURL = httpURL.replace(credentials[0] + ":" + credentials[1] + "@", "");
        }
        //
        ResponseEntity<String> responseEntity = restTemplate.exchange(
                httpURL,
                HttpMethod.GET,
                new HttpEntity<>(headers),
                String.class
        );

        String viewList = responseEntity.getBody();
        return viewList;
    }

    public static String[] parseCredentials(String urlString) {
        try {
            URI uri = new URI(urlString);
            // Extract the user-info part (username:password)
            String userInfo = uri.getUserInfo();

            if (userInfo != null && userInfo.contains(":")) {
                String[] credentials = userInfo.split(":", 2);
                String username = credentials[0];
                String password = credentials[1];
                //
                logger.info("Username: " + username);
                logger.info("Password: " + password);
                //
                return credentials;
            } else {
                logger.info("No credentials found in the URL.");
            }
        } catch (URISyntaxException e) {
            logger.info("Invalid URL format: " + e.getMessage());
            throw new RuntimeException("Invalid URL format: " + e.getMessage());
        }
        return null;
    }
}
/*
https://www.baeldung.com/spark-framework-rest-api
*/