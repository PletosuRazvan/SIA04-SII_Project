package org.sparkservices;

import org.junit.jupiter.api.Test;
import org.spark.service.rest.QueryRESTDataService;

public class TestURIwithCreds {
    @Test
    public void testCredentials() {
        String urlString = "http://localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView";
        QueryRESTDataService.parseCredentials(urlString);
        urlString = "http://developer:iis@localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView";
        QueryRESTDataService.parseCredentials(urlString);
        urlString = "12345";
        QueryRESTDataService.parseCredentials(urlString);
    }

    @Test
    public void testCredentials2() {
        String urlString = "http://developer:iis@localhost:8090/DSA-SQL-JDBCService/rest/customers/CustomerView";
        System.out.println(urlString);
        String[] credentials = QueryRESTDataService.parseCredentials(urlString);
        urlString = urlString.replace(credentials[0] + ":" + credentials[1] + "@", "");
        System.out.println(urlString);
    }
}
