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
public class JarEntryDuplicateCandidate extends DuplicateCandidate {

	@Key("tag_ss")
	private List<String> tags = new ArrayList<>();

	/**
	 * containing jar. null for a jar itself
	 */
	@Key("jar_s")
	private String jar;

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

	@Override
	public String toString() {
		return getId() + " tags:" + Arrays.toString(getTags().toArray()) + " length:" + getLength() + " md5:" + getMd5();
	}

	public static class SolrSelectResponse extends GenericSolrSelectResponse<JarEntryDuplicateCandidate> {
	}
}
