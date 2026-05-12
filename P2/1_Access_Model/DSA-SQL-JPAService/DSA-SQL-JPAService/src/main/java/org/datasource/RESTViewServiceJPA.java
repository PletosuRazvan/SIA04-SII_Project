package org.datasource;

import org.datasource.jpa.JPADataSourceConnector;
import org.datasource.jpa.views.regions.NocRegionView;
import org.datasource.jpa.views.regions.NocRegionViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;

/*	REST Service URL
	http://localhost:8091/DSA_SQL_JPAService/rest/olympics/NocRegionView
*/
@RestController
@RequestMapping("/olympics")
public class RESTViewServiceJPA {
	private static Logger logger = Logger.getLogger(RESTViewServiceJPA.class.getName());

	@RequestMapping(value = "/ping", method = RequestMethod.GET,
			produces = {MediaType.TEXT_PLAIN_VALUE})
	@ResponseBody
	public String pingDataSource() {
		logger.info(">>>> DSA-SQL-JPAService:: RESTViewService is Up!");
		return "Ping response from DSA-SQL-JPAService (Olympics PostgreSQL)!";
	}

	@RequestMapping(value = "/NocRegionView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<NocRegionView> get_NocRegionView() {
		List<NocRegionView> viewList = this.regionViewBuilder.build().getRegionViewList();
		return viewList;
	}

	// Set-up
	@Autowired private JPADataSourceConnector dataSourceConnector;
	@Autowired private NocRegionViewBuilder regionViewBuilder;
}