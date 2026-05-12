package org.datasource.poi;

//import org.apache.poi.hssf.usermodel.HSSFDateUtil;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.util.*;
import java.util.logging.Logger;

import static org.apache.poi.ss.usermodel.DateUtil.isCellDateFormatted;

/*
* Convert Excel-Tabelar model based on sheet with multiple rows to a minimalist OO Model:
* Collection(List) of named-tuples, a tuple to be represented as a key(name)-value Map:
* XSSF [Sheet->[*]Row] mapped to List<Map<String,Object>>
* - each Excel Row will be mapped to Map<String,Object>
* - the Excel Sheet (of Rows) will be converted into a Collection-List (of Maps)
 */
public class XLSXTupleViewBuilder {
	private static Logger logger = Logger.getLogger(XLSXTupleViewBuilder.class.getName());
	// Options
	private String worksheetName;
	//
	private List<Map<String, Object>> tupleList;
	//
	private XLSXResourceFileDataSourceConnector dataSourceConnector;
	private XSSFWorkbook workbook;
	private XSSFSheet worksheet;
	private Integer rowHeaderNumber = 0;
	//
	public XLSXTupleViewBuilder(XLSXResourceFileDataSourceConnector dataSourceConnector) throws Exception{
		this.setDataSourceConnector(dataSourceConnector);

	}
	public XLSXTupleViewBuilder() {}

	public void setDataSourceConnector(XLSXResourceFileDataSourceConnector dataSourceConnector) throws Exception{
		this.dataSourceConnector = dataSourceConnector;
		//FileInputStream file = this.dataSourceConnector.getXLSXFileInputStream();
		File file = this.dataSourceConnector.getXLSXFile();
		this.workbook = new XSSFWorkbook(file);
		// By default, load the first sheet
		this.worksheet = this.workbook.getSheetAt(0);
	}

	/* If the target Sheet is not the first, get the Sheet with name or index provided*/
	public void setWorksheet(String worksheetName) {
		this.worksheet = this.workbook.getSheet(worksheetName);
		this.worksheetName = this.worksheet.getSheetName();
	}
	public void setWorksheet(Integer worksheetNo) {
		this.worksheet = this.workbook.getSheetAt(worksheetNo);
		this.worksheetName = this.worksheet.getSheetName();
	}
	public void setRowHeaderNumber(int rowHeaderNumber) {
		this.rowHeaderNumber = rowHeaderNumber;
	}
	//
	public List<Map<String, Object>> getTupleList() {
		return tupleList;
	}
	
	// Building process
	private ListTupleAdaptor listTupleAdaptor = new ListTupleAdaptor();
	public XLSXTupleViewBuilder build() throws Exception {
		if (this.workbook == null)
			throw new Exception("Workbook not set: call setDataSourceConnector first!");
		//
		this.listTupleAdaptor.setRowHeaderNumber(this.rowHeaderNumber);
		this.tupleList = this.listTupleAdaptor.map(this.worksheet);
		return this;
	}	
}

class ListTupleAdaptor{
	private static Logger logger = Logger.getLogger(ListTupleAdaptor.class.getName());

	private int rowHeaderNumber = 0;
	private TupleAdaptor tupleAdaptor = new TupleAdaptor();

	public void setRowHeaderNumber(int rowHeaderNumber) {
		this.rowHeaderNumber = rowHeaderNumber;
	}

	public List<Map<String, Object>> map(XSSFSheet sheet){
		List<Map<String, Object>> tuples = new ArrayList<>();
		List<String> sheetHeader = getSheetHeader(sheet);
		//
		logger.info("==== HEADER: " + sheetHeader);
		//
		Iterator<Row> rowIterator = sheet.rowIterator();
		while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            if (row.getRowNum() > this.rowHeaderNumber) // Not processing header
				tuples.add(tupleAdaptor.map(row, sheetHeader));
        }
		
		return tuples;
	}
	
	private List<String> getSheetHeader(XSSFSheet sheet){
		List<String> sheetHeader = new ArrayList<>();
		Row row = sheet.getRow(this.rowHeaderNumber);
		Iterator<Cell> cellIterator = row.cellIterator();
		while (cellIterator.hasNext()) {
			Cell cell = cellIterator.next();
			sheetHeader.add(cell.getStringCellValue());
		}
		return sheetHeader;
	}
}

class TupleAdaptor{
	public Map<String, Object> map(Row row, List<String> sheetHeader){
		Map<String, Object> tuple = new HashMap<>();
		
		List<Object> rowValues = new ArrayList<>();
		Iterator<Cell> cellIterator = row.cellIterator();
        // get row values
        while (cellIterator.hasNext()) {
            Cell cell = cellIterator.next();
            // cell value types
            switch (cell.getCellType()) {
                // case Cell.CELL_TYPE_NUMERIC:
            	case NUMERIC:
                {
                    if (isCellDateFormatted(cell)){
                    	Date dateValue = cell.getDateCellValue();
                    	// Date
                    	rowValues.add(dateValue);
                    }else{
                    	// Number
                    	Number numberValue = cell.getNumericCellValue();
                    	rowValues.add(numberValue);
                    	//System.out.println("Effective number type: " + numberValue.getClass().getName());
                    }
                    break;
                }
                // case Cell.CELL_TYPE_STRING:
            	case STRING:
                {
                    // Plain String
                	String stringValue = cell.getStringCellValue();
                	rowValues.add(stringValue);
                    break;
                }
				// CellType.BLANK
				case BLANK:
				{
					rowValues.add(null);
					break;
				}
            }

        }
        // build tuple
        for (int i=0; i < sheetHeader.size(); i++){
			try {
				tuple.put(sheetHeader.get(i), rowValues.get(i));
			}catch (IndexOutOfBoundsException ex){
				tuple.put(sheetHeader.get(i), null);
			}
        }
        return tuple;
	}
}