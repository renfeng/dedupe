package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

/**
 * extends this class to add doc fields
 *
 * https://cwiki.apache.org/confluence/display/solr/Apache+Solr+Reference+Guide
 * 
 * Created by renfeng on 8/15/15.
 */
public class SolrDocBase {

	/*
	 * TODO rename and replace the values in solr index
	 */
	public static final String DUPLICATE_CANDIDATE_TYPE =
			"hu.dushu.developers.dedupe.DuplicateCandicate";
	public static final String JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE =
			"hu.dushu.developers.dedupe.jar.JarResourceDuplicateCandidate";

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
