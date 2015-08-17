package hu.dushu.developers.dedupe.jar;

import com.google.api.client.util.Key;
import hu.dushu.developers.dedupe.DuplicateCandidate;
import hu.dushu.developers.dedupe.GenericSolrSelectResponse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by renfeng on 8/15/15.
 */
public class JarEntryDuplicateCandidate extends DuplicateCandidate implements Comparable<JarEntryDuplicateCandidate> {

	@Key("tag_ss")
	private List<String> tags = new ArrayList<>();

	/**
	 * containing jar. null for a jar itself
	 * <p/>
	 * context
	 * http://download.java.net/jdk7/archive/b123/docs/api/java/net/JarURLConnection.html
	 */
	@Key("jar_s")
	private String jar;

	@Key("entry_s")
	private String entry;

	public JarEntryDuplicateCandidate() {
		setType(JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE);
	}

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

	public static class SolrSelectResponse extends GenericSolrSelectResponse<JarEntryDuplicateCandidate> {
	}
}
