package hu.dushu.developers.dedupe.jar;

import com.google.gson.annotations.SerializedName;
import hu.dushu.developers.dedupe.DuplicateCandidate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by renfeng on 8/15/15.
 */
public class JarEntryDuplicateCandidate
		extends DuplicateCandidate
		implements Comparable<JarEntryDuplicateCandidate> {

	@SerializedName("tag_ss")
	private List<String> tags = new ArrayList<>();

	/**
	 * containing jar. null for a jar itself
	 * <p/>
	 * context
	 * http://download.java.net/jdk7/archive/b123/docs/api/java/net/JarURLConnection.html
	 */
	@SerializedName("jar_s")
	private String jar;

	@SerializedName("entry_s")
	private String entry;

	/*
	 * http://docs.oracle.com/javase/7/docs/api/java/lang/Package.html
	 * https://docs.oracle.com/javase/tutorial/deployment/jar/packageman.html
	 * http://stackoverflow.com/questions/20994766/jar-manifest-file-difference-between-specification-and-implementation
	 */

	@SerializedName("specificationTitle_s")
	private String specificationTitle;

	@SerializedName("specificationVender_s")
	private String specificationVender;

	@SerializedName("specificationVersion_s")
	private String specificationVersion;

	public List<String> getTags() {
		return tags;
	}

	public void setTags(List<String> tags) {
		this.tags = tags;
	}

	public String getJar() {
		return jar;
	}

	public void setJar(String jar) {
		this.jar = jar;
	}

	public String getEntry() {
		return entry;
	}

	public void setEntry(String entry) {
		this.entry = entry;
	}

	public String getSpecificationTitle() {
		return specificationTitle;
	}

	public void setSpecificationTitle(String specificationTitle) {
		this.specificationTitle = specificationTitle;
	}

	public String getSpecificationVender() {
		return specificationVender;
	}

	public void setSpecificationVender(String specificationVender) {
		this.specificationVender = specificationVender;
	}

	public String getSpecificationVersion() {
		return specificationVersion;
	}

	public void setSpecificationVersion(String specificationVersion) {
		this.specificationVersion = specificationVersion;
	}

	@Override
	public String toString() {
		return getId() + " tags:" + Arrays.toString(getTags().toArray()) + " length:" + getLength() + " md5:" + getMd5();
	}

	@Override
	public int compareTo(JarEntryDuplicateCandidate o) {
		int result = -Long.compare(getLength(), o.getLength());
		if (result == 0) {
			result = getEntry().compareTo(o.getEntry());
		}
		return result;
	}

	public static class SolrSelectResponse
			extends work.fair24.solr.SolrSelectResponse<JarEntryDuplicateCandidate, JarDedupeFacetFields> {
	}
}
