package ch.uzh.ddis.katts.query.output;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import ch.uzh.ddis.katts.bolts.Bolt;
import ch.uzh.ddis.katts.bolts.output.SystemOutputBolt;
import ch.uzh.ddis.katts.bolts.output.SystemOutputConfiguration;

/**
 * This class implements a output node configuration for a system print out node.
 * 
 * @author Thomas Hunziker
 * 
 */
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class SystemOutput extends AbstractOutput implements SystemOutputConfiguration {

	private static final long serialVersionUID = 1L;

	@Override
	public boolean validate() {
		return true;
	}

	@Override
	public Bolt getBolt() {
		SystemOutputBolt bolt = new SystemOutputBolt();

		return bolt;
	}
}
