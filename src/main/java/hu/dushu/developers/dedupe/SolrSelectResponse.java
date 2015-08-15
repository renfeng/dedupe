package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

/**
 * Created by renfeng on 8/15/15.
 */
public class SolrSelectResponse {

//	@Key
//	private SolrResponseHeader responseHeader;

	@Key
	private SolrResponse response;

	@Key("facet_counts")
	private SolrFacetCounts facetCounts;

	public SolrResponse getResponse() {
		return response;
	}

	public void setResponse(SolrResponse response) {
		this.response = response;
	}

	public SolrFacetCounts getFacetCounts() {
		return facetCounts;
	}

	public void setFacetCounts(SolrFacetCounts facetCounts) {
		this.facetCounts = facetCounts;
	}
}
