package org.datasource.jpa.views.regions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import jakarta.persistence.*;
import java.io.Serializable;

/**
 * JPA Entity for the NOC_REGIONS PostgreSQL table.
 * Maps Olympic NOC codes to country/region names.
 */
@Data @AllArgsConstructor
@NoArgsConstructor(force = true)
@Entity @Table(name = "noc_regions")
public class NocRegionView implements Serializable {
    private static final long serialVersionUID = 1L;

    @Id
    @Column(name = "noc")
    private String noc;

    @Column(name = "region")
    private String region;

    @Column(name = "notes")
    private String notes;
}

