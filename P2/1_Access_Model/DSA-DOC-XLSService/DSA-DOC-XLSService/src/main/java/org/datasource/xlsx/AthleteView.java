package org.datasource.xlsx;

import com.poiji.annotation.ExcelCellName;
import com.poiji.annotation.ExcelSheet;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor(force = true)
@ExcelSheet("Athletes")
public class AthleteView {
    @ExcelCellName("ID")
    private Long id;

    @ExcelCellName("Name")
    private String name;

    @ExcelCellName("Sex")
    private String sex;

    @ExcelCellName("Height")
    private Double height;

    @ExcelCellName("Weight")
    private Double weight;

    public AthleteView(Long id, String name, String sex, Double height, Double weight) {
        this.id = id;
        this.name = name;
        this.sex = sex;
        this.height = height;
        this.weight = weight;
    }

    // explicit getters for Lombok compatibility
    public Long getId() { return id; }
    public String getName() { return name; }
    public String getSex() { return sex; }
    public Double getHeight() { return height; }
    public Double getWeight() { return weight; }
}

