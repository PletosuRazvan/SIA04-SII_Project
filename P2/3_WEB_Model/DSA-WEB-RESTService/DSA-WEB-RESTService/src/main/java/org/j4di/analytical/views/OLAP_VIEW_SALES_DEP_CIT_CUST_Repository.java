package org.j4di.analytical.views;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OLAP_VIEW_SALES_DEP_CIT_CUST_Repository extends JpaRepository<OLAP_VIEW_SALES_DEP_CIT_CUST, Long> {

    @Query("SELECT o FROM OLAP_VIEW_SALES_DEP_CIT_CUST o")
    List<OLAP_VIEW_SALES_DEP_CIT_CUST> get_OLAP_VIEW_SALES_DEP_CIT_CUST();
}