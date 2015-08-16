package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

/**
 * Created by renfeng on 8/15/15.
 */
public class SolrDoc {

	/*
	 * TODO fix typo and replace the values in solr index
	 */
	public static final String DUPLICATE_CANDIDATE_TYPE = "hu.dushu.developers.dedupe.DuplicateCandicate";

	@Key
	private String id;

	@Key("type_s")
	private String type;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
}
