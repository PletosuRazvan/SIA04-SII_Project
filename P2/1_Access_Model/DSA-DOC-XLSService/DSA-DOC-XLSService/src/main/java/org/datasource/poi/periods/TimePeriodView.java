package org.datasource.poi.periods;

import com.poiji.annotation.ExcelCellName;
import com.poiji.annotation.ExcelSheet;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data @AllArgsConstructor @NoArgsConstructor(force = true)
@ExcelSheet("Periods")
public class TimePeriodView {
	//@ExcelRow private int rowIndex;
	@ExcelCellName("Period")
	private String period;
	@ExcelCellName("StartDate")
	private Date startDate;
	@ExcelCellName("EndDate")
	private Date endDate;
}
