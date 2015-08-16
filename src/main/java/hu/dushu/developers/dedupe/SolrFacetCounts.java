package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

/**
 * Created by renfeng on 8/15/15.
 */
public class SolrFacetCounts {

//	@Key("facet_queries")
//	private SolrFacetQueries facetQueries;

	@Key("facet_fields")
	private SolrFacetFields facetFields;

//	@Key("facet_dates")
//	private SolrFacetDates facetDates;

//	@Key("facet_ranges")
//	private SolrFacetRanges facetRanges;

	public SolrFacetFields getFacetFields() {
		return facetFields;
	}

	public void setFacetFields(SolrFacetFields facetFields) {
		this.facetFields = facetFields;
	}
}
