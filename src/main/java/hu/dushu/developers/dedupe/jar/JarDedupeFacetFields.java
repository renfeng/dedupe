package hu.dushu.developers.dedupe.jar;

import com.google.api.client.util.Key;
import hu.dushu.developers.dedupe.DedupeFacetFields;

import java.util.List;

/**
 * Created by frren on 8/17/2015.
 */
public class JarDedupeFacetFields extends DedupeFacetFields {

	@Key("jar_s")
	private List<Object> jar;

	@Key("entry_s")
	private List<Object> entry;

	@Key("id")
	private List<Object> id;

	@Key("specificationTitle_s")
	private List<Object> specificationTitle;

	@Key("specificationVersion_s")
	private List<Object> specificationVersion;

	public List<Object> getJar() {
		return jar;
	}

	public void setJar(List<Object> jar) {
		this.jar = jar;
	}

	public List<Object> getEntry() {
		return entry;
	}

	public void setEntry(List<Object> entry) {
		this.entry = entry;
	}

	public List<Object> getId() {
		return id;
	}

	public void setId(List<Object> id) {
		this.id = id;
	}

	public List<Object> getSpecificationTitle() {
		return specificationTitle;
	}

	public void setSpecificationTitle(List<Object> specificationTitle) {
		this.specificationTitle = specificationTitle;
	}

	public List<Object> getSpecificationVersion() {
		return specificationVersion;
	}

	public void setSpecificationVersion(List<Object> specificationVersion) {
		this.specificationVersion = specificationVersion;
	}
}
