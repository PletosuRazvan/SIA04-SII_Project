package org.j4di.integration.views;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

import java.time.LocalDate;

@Getter
@Entity
@Immutable
@Table(name = "OLAP_FACTS_SALES_AMOUNT")
public class OLAP_FACTS_SALES_AMOUNT {
    @Id
    private Long customerId;
    private Long productCode;
    private LocalDate invoiceDate;
    private Long sales_amount;
}