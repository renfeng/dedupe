package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

/**
 * Created by renfeng on 8/15/15.
 */
public class GenericSolrSelectResponse<T extends SolrDoc> {

//	@Key
//	private SolrResponseHeader responseHeader;

	@Key
	private GenericSolrResponse<T> response;

	@Key("facet_counts")
	private SolrFacetCounts facetCounts;

	public GenericSolrResponse<T> getResponse() {
		return response;
	}

	public void setResponse(GenericSolrResponse<T> response) {
		this.response = response;
	}

	public SolrFacetCounts getFacetCounts() {
		return facetCounts;
	}

	public void setFacetCounts(SolrFacetCounts facetCounts) {
		this.facetCounts = facetCounts;
	}
}
