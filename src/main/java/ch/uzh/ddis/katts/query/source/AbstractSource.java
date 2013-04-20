package ch.uzh.ddis.katts.query.source;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementRef;
import javax.xml.bind.annotation.XmlElementRefs;
import javax.xml.bind.annotation.XmlElementWrapper;

import ch.uzh.ddis.katts.query.AbstractNode;

/**
 * This class is an abstract implementation of an source node.
 * 
 * @see Source
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
public abstract class AbstractSource extends AbstractNode implements Source {

	@XmlElementWrapper(name = "files")
	@XmlElement(name = "file")
	private List<File> files = new ArrayList<File>();

	public List<File> getFiles() {
		return files;
	}

	public void setFiles(List<File> files) {
		this.files = files;
	}

	@Override
	public int getParallelism() {
		// We need as many bolts, as we have files in our list.
		return this.files.size();
	}

	@Override
	public boolean validate() {
		return true;
	}

}
