package ch.uzh.ddis.katts.utils;
import java.io.File;
import java.io.IOException;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.SchemaOutputResolver;
import javax.xml.transform.Result;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.compress.archivers.ArchiveException;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import ch.uzh.ddis.katts.query.Query;


public class CreateXSDSchemaForXmlQuery {
	
	
	public static void main(String[] args) throws ArchiveException, IOException, AlreadyAliveException, InvalidTopologyException, JAXBException {
		JAXBContext jaxbContext = Query.getJAXBContext();
		SchemaOutputResolver sor = new SchemaOutputResolver() {
			public Result createOutput(String namespaceURI, String suggestedFileName)
					throws IOException {
				File file = new File(suggestedFileName);
				StreamResult result = new StreamResult(file);
				result.setSystemId(file.toURI().toURL().toString());
				return result;
			}
		};
		jaxbContext.generateSchema(sor);
	}
	
}
