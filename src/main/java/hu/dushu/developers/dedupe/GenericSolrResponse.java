package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

import java.util.List;

/**
 * Created by renfeng on 8/15/15.
 */
public class GenericSolrResponse<T extends SolrDoc> {

	@Key
	private int numFound;

	@Key
	private int start;

	@Key
	private List<T> docs;

	public List<T> getDocs() {
		return docs;
	}

	public void setDocs(List<T> docs) {
		this.docs = docs;
	}

	public int getNumFound() {
		return numFound;
	}

	public void setNumFound(int numFound) {
		this.numFound = numFound;
	}

	public int getStart() {
		return start;
	}

	public void setStart(int start) {
		this.start = start;
	}
}
