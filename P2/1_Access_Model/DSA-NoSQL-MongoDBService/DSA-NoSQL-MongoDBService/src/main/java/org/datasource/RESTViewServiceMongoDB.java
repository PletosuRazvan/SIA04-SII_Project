package org.datasource;

import org.datasource.mongodb.views.results.ResultView;
import org.datasource.mongodb.views.results.ResultViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;


/*	REST Service URL
	http://localhost:8093/DSA-NoSQL-MongoDBService/rest/olympics/ResultView
*/
@RestController @RequestMapping("/olympics")
public class RESTViewServiceMongoDB {
	private static Logger logger = Logger.getLogger(RESTViewServiceMongoDB.class.getName());

	@RequestMapping(value = "/ping", method = RequestMethod.GET,
		produces = {MediaType.TEXT_PLAIN_VALUE})
	@ResponseBody
	public String pingDataSource() {
		logger.info(">>>> RESTViewServiceMongoDB (Olympics) is Up!");
		return "Ping response from RESTViewServiceMongoDB (Olympics)!";
	}

	@RequestMapping(value = "/ResultView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<ResultView> get_ResultView() throws Exception {
		List<ResultView> viewList = this.resultViewBuilder.build().getResultViewList();
		return viewList;
	}

	// Set-up
	@Autowired private ResultViewBuilder resultViewBuilder;
}