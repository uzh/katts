package ch.uzh.ddis.katts.query.source;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlTransient;

/**
 * This class represents a file source. The file source defines the format
 * and the type of the file.
 * 
 * @author Thomas Hunziker
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class File implements Serializable{
	
	private static final long serialVersionUID = 1L;

	@XmlAttribute(required=true)
	private String path;
	
	@XmlAttribute(required=false)
	private String zipFileEntry;
	
	@XmlAttribute(required=false)
	private boolean isZipped = false;
	
	@XmlAttribute(required=false)
	private String mimeType = "text/comma-separated-values";
	
	@XmlAttribute(required=false)
	private String csvFieldDelimiter = ",";
	
	/** 0-based line number. if > 0, only all lines up to this line (exclusive) will be read. */
	@XmlAttribute
	private long readToLineNo = 0;

	@XmlTransient
	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@XmlTransient
	public String getZipFileEntry() {
		return zipFileEntry;
	}

	public void setZipFileEntry(String zipFileEntry) {
		this.zipFileEntry = zipFileEntry;
	}

	@XmlTransient
	public String getCsvFieldDelimiter() {
		return csvFieldDelimiter;
	}

	public void setCsvFieldDelimiter(String csvFieldDelimiter) {
		this.csvFieldDelimiter = csvFieldDelimiter;
	}

	@XmlTransient
	public boolean isZipped() {
		return isZipped;
	}

	public void setZipped(boolean isZipped) {
		this.isZipped = isZipped;
	}

	@XmlTransient
	public String getMimeType() {
		return mimeType;
	}

	public void setMimeType(String mimeType) {
		this.mimeType = mimeType;
	}

	/**
	 * {@link File#readToLineNo}
	 * @return the toLineNo
	 */
	public long getReadToLineNo() {
		return readToLineNo;
	}

	/**
	 * {@link File#readToLineNo}
	 * @param readToLineNo the toLineNo to set
	 */
	public void setReadToLineNo(long readToLineNo) {
		this.readToLineNo = readToLineNo;
	}
}
