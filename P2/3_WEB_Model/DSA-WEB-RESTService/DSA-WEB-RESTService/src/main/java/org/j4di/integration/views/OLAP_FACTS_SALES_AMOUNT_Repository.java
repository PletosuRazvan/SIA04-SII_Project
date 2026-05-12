package org.j4di.integration.views;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OLAP_FACTS_SALES_AMOUNT_Repository extends JpaRepository<OLAP_FACTS_SALES_AMOUNT, Long> {

    @Query("SELECT o FROM OLAP_FACTS_SALES_AMOUNT o")
    List<OLAP_FACTS_SALES_AMOUNT> get_OLAP_FACTS_SALES_AMOUNT();
}