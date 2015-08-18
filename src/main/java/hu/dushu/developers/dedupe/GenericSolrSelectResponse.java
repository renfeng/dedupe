package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

/**
 * Created by renfeng on 8/15/15.
 */
public class GenericSolrSelectResponse<T extends SolrDocBase, F extends SolrFacetFieldsBase> {

//	@Key
//	private SolrResponseHeader responseHeader;

	@Key
	private GenericSolrResponse<T> response;

	@Key("facet_counts")
	private GenericSolrFacetCounts<F> facetCounts;

	public GenericSolrResponse<T> getResponse() {
		return response;
	}

	public void setResponse(GenericSolrResponse<T> response) {
		this.response = response;
	}

	public GenericSolrFacetCounts<F> getFacetCounts() {
		return facetCounts;
	}

	public void setFacetCounts(GenericSolrFacetCounts<F> facetCounts) {
		this.facetCounts = facetCounts;
	}
}
