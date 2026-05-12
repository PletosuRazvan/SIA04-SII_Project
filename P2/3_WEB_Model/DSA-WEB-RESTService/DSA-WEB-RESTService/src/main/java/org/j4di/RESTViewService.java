package org.j4di;

import org.j4di.analytical.views.*;
import org.j4di.integration.views.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.logging.Logger;

/*	REST Service URL - Olympics Analytical Views from SparkSQL via Hive JDBC
	http://localhost:8096/DSA-WEB-RESTService/rest/OLAP/OLAP_CONSOLIDARE_OLIMPICA
	http://localhost:8096/DSA-WEB-RESTService/rest/OLAP/OLAP_FACTS_MEDALS
	http://localhost:8096/DSA-WEB-RESTService/rest/OLAP/OLAP_VIEW_CUBE_COUNTRY_SPORT
	http://localhost:8096/DSA-WEB-RESTService/rest/OLAP/OLAP_VIEW_RANK_ATHLETES
	http://localhost:8096/DSA-WEB-RESTService/rest/OLAP/OLAP_VIEW_COUNTRY_SPORT_ATHLETE
*/
@RestController
@RequestMapping("/OLAP")
public class RESTViewService {
	private static Logger logger = Logger.getLogger(RESTViewService.class.getName());

	@RequestMapping(value = "/ping", method = RequestMethod.GET,
			produces = {MediaType.TEXT_PLAIN_VALUE})
	@ResponseBody
	public String pingDataSource() {
		logger.info(">>>> DSA-WEB-RESTService (Olympics OLAP) is Up!");
		return "Ping response from DSA-WEB-RESTService (Olympics OLAP)!";
	}

	// ============ Integration Views ============

	@Autowired private OLAP_CONSOLIDARE_OLIMPICA_Repository consolidareRepository;
	@Autowired private OLAP_FACTS_MEDALS_Repository factsRepository;

	@GetMapping(value = "/OLAP_CONSOLIDARE_OLIMPICA",
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<OLAP_CONSOLIDARE_OLIMPICA> get_OLAP_CONSOLIDARE_OLIMPICA() {
		List<OLAP_CONSOLIDARE_OLIMPICA> viewList = this.consolidareRepository.get_OLAP_CONSOLIDARE_OLIMPICA();
		return viewList;
	}

	@GetMapping(value = "/OLAP_FACTS_MEDALS",
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<OLAP_FACTS_MEDALS> get_OLAP_FACTS_MEDALS() {
		List<OLAP_FACTS_MEDALS> viewList = this.factsRepository.get_OLAP_FACTS_MEDALS();
		return viewList;
	}

	// ============ Analytical Views ============

	@Autowired private OLAP_VIEW_CUBE_COUNTRY_SPORT_Repository cubeRepository;
	@Autowired private OLAP_VIEW_RANK_ATHLETES_Repository rankRepository;
	@Autowired private OLAP_VIEW_COUNTRY_SPORT_ATHLETE_Repository countryRepository;

	@GetMapping(value = "/OLAP_VIEW_CUBE_COUNTRY_SPORT",
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<OLAP_VIEW_CUBE_COUNTRY_SPORT> get_OLAP_VIEW_CUBE_COUNTRY_SPORT() {
		List<OLAP_VIEW_CUBE_COUNTRY_SPORT> viewList = this.cubeRepository.get_OLAP_VIEW_CUBE_COUNTRY_SPORT();
		return viewList;
	}

	@GetMapping(value = "/OLAP_VIEW_RANK_ATHLETES",
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<OLAP_VIEW_RANK_ATHLETES> get_OLAP_VIEW_RANK_ATHLETES() {
		List<OLAP_VIEW_RANK_ATHLETES> viewList = this.rankRepository.get_OLAP_VIEW_RANK_ATHLETES();
		return viewList;
	}

	@GetMapping(value = "/OLAP_VIEW_COUNTRY_SPORT_ATHLETE",
			produces = {MediaType.APPLICATION_JSON_VALUE, MediaType.APPLICATION_XML_VALUE})
	@ResponseBody
	public List<OLAP_VIEW_COUNTRY_SPORT_ATHLETE> get_OLAP_VIEW_COUNTRY_SPORT_ATHLETE() {
		List<OLAP_VIEW_COUNTRY_SPORT_ATHLETE> viewList = this.countryRepository.get_OLAP_VIEW_COUNTRY_SPORT_ATHLETE();
		return viewList;
	}
}