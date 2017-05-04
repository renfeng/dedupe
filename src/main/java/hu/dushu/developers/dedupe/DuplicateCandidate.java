package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;
import work.fair24.solr.SolrDocument;

/**
 * Created by renfeng on 8/15/15.
 */
public class DuplicateCandidate extends SolrDocument {

	@Key("directory_b")
	private boolean directory;

	@Key("length_l")
	private Long length;

	@Key("md5_s")
	private String md5;

	public Long getLength() {
		return length;
	}

	public void setLength(Long length) {
		this.length = length;
	}

	public String getMd5() {
		return md5;
	}

	public void setMd5(String md5) {
		this.md5 = md5;
	}

	public boolean isDirectory() {
		return directory;
	}

	public void setDirectory(boolean directory) {
		this.directory = directory;
	}

	@Override
	public String toString() {
		return getId() + " directory:" + isDirectory() + " length:" + getLength() + " md5:" + getMd5();
	}

	public static class SolrSelectResponse extends work.fair24.solr.SolrSelectResponse<DuplicateCandidate, DedupeFacetFields> {
	}
}
