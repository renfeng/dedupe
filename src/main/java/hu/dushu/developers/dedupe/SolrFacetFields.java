package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

import java.util.List;

/**
 * Created by renfeng on 8/15/15.
 */
public class SolrFacetFields {

	@Key("length_l")
	private List<Object> length;

	public List<Object> getLength() {
		return length;
	}

	public void setLength(List<Object> length) {
		this.length = length;
	}
}
