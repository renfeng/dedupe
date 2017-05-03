package hu.dushu.developers.dedupe;

import com.google.gson.annotations.SerializedName;
import work.fair24.solr.SolrFacetFields;

import java.util.List;

/**
 * Created by frren on 8/17/2015.
 */
public class DedupeFacetFields extends SolrFacetFields {

	@SerializedName("length_l")
	private List<Object> length;

	public List<Object> getLength() {
		return length;
	}

	public void setLength(List<Object> length) {
		this.length = length;
	}
}
