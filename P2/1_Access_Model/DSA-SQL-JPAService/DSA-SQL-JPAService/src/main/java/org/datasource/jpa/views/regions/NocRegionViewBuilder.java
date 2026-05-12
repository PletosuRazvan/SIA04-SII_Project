package org.datasource.jpa.views.regions;

import org.datasource.jpa.JPADataSourceConnector;
import org.springframework.stereotype.Service;

import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class NocRegionViewBuilder {
    private static Logger logger = Logger.getLogger(NocRegionViewBuilder.class.getName());

    protected String JPQL_REGIONS_SELECT =
            "SELECT NEW org.datasource.jpa.views.regions.NocRegionView("
            + "r.noc, r.region, r.notes) "
            + "FROM NocRegionView r";

    protected List<NocRegionView> regionViewList = new ArrayList<>();

    public List<NocRegionView> getRegionViewList() {
        return regionViewList;
    }

    public NocRegionViewBuilder build() {
        return this.select();
    }

    protected NocRegionViewBuilder select() {
        EntityManager em = dataSourceConnector.getEntityManager();
        Query viewQuery = em.createQuery(JPQL_REGIONS_SELECT);
        this.regionViewList = viewQuery.getResultList();
        logger.info("Built " + regionViewList.size() + " NocRegionView records");
        return this;
    }

    protected JPADataSourceConnector dataSourceConnector;

    public NocRegionViewBuilder(JPADataSourceConnector dataSourceConnector) {
        this.dataSourceConnector = dataSourceConnector;
    }
}

