package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

import java.util.List;

/**
 * Created by renfeng on 8/15/15.
 */
public class SolrDelete {

	@Key
	private List<String> delete;

	public List<String> getDelete() {
		return delete;
	}

	public void setDelete(List<String> delete) {
		this.delete = delete;
	}
}
