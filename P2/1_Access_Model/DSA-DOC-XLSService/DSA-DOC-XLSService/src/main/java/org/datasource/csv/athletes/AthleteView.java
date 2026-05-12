package org.datasource.csv.athletes;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor(force = true)
public class AthleteView {
    private Long id;
    private String name;
    private String sex;
    private Double height;
    private Double weight;

    public AthleteView(Long id, String name, String sex, Double height, Double weight) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.height = height;
        this.weight = weight;
    }
}
