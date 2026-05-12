package org.datasource.poi;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.StandardCopyOption;
import java.util.logging.Logger;

//@Component // disabled - using Olympics XLSX builder instead
public class XLSXResourceFileDataSourceConnector {
	private static Logger logger = Logger.getLogger(XLSXResourceFileDataSourceConnector.class.getName());

	@Value("${xlsx.data.source.file.path}")
	protected String XLSFilePath;
	//
	protected File XLSfile;
	
	public String getXLSFilePath() {
		return XLSFilePath;
	}

	public File getXLSXFile() throws Exception {
		logger.info("Filepath accessed: " + this.XLSFilePath);
		if (this.XLSfile == null) {
			this.XLSfile = new File(this.XLSFilePath);
			//
			if (!this.XLSfile.exists()) {
				this.XLSfile = new File("temp.xlsx");
				java.nio.file.Files.copy(
						new ClassPathResource(XLSFilePath).getInputStream(),
						this.XLSfile.toPath(),
						StandardCopyOption.REPLACE_EXISTING);
				logger.info("... loaded from ClassPathResource!");
			}else
				logger.info("... loaded from local FileSystem!");
		}
		return this.XLSfile;
	}
}