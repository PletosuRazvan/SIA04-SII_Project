package org.datasource;

import org.datasource.xlsx.AthleteView;
import org.datasource.xlsx.GamesView;
import org.datasource.xlsx.OlympicsXLSXViewBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.logging.Logger;


/*	REST Service URL
	http://localhost:8094/DSA-DOC-XLSService/rest/olympics/AthleteView
	http://localhost:8094/DSA-DOC-XLSService/rest/olympics/GamesView
*/
@RestController @RequestMapping("/olympics")
public class RESTViewServiceXLS {
	private static Logger logger = Logger.getLogger(RESTViewServiceXLS.class.getName());

	@RequestMapping(value = "/ping", method = RequestMethod.GET,
			produces = {MediaType.TEXT_PLAIN_VALUE})
	@ResponseBody
	public String pingDataSource() {
		logger.info(">>>> REST XLSX Data Source is Up!");
		return "PING response from DSA-DOC-XLSService (Olympics XLSX)!";
	}

	@RequestMapping(value = "/AthleteView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<AthleteView> get_AthleteView() throws Exception {
		List<AthleteView> viewList = this.xlsxViewBuilder.buildAthletes().getAthleteViewList();
		logger.info("AthleteView returned " + viewList.size() + " records from XLSX");
		return viewList;
	}

	@RequestMapping(value = "/GamesView", method = RequestMethod.GET,
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<GamesView> get_GamesView() throws Exception {
		List<GamesView> viewList = this.xlsxViewBuilder.buildGames().getGamesViewList();
		logger.info("GamesView returned " + viewList.size() + " records from XLSX");
		return viewList;
	}

	// Set-up
	@Autowired private OlympicsXLSXViewBuilder xlsxViewBuilder;
}