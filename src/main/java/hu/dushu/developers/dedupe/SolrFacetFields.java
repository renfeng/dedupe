package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

import java.util.List;

/**
 * Created by renfeng on 8/15/15.
 */
public class SolrFacetFields {

	@Key("length_l")
	private List<Object> length;

	@Key("jar_s")
	private List<Object> jar;

	public List<Object> getLength() {
		return length;
	}

	public List<Object> getJar() {
		return jar;
	}

	public void setJar(List<Object> jar) {
		this.jar = jar;
	}

	public void setLength(List<Object> length) {

		this.length = length;
	}
}
