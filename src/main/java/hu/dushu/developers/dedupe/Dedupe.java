package hu.dushu.developers.dedupe;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created by renfeng on 8/15/15.
 */
public class Dedupe {

	static final Logger logger = LoggerFactory.getLogger(Dedupe.class);

	protected static final JacksonFactory jsonFactory = new JacksonFactory();

	protected static final HttpRequestFactory factory = new NetHttpTransport().createRequestFactory(
			new HttpRequestInitializer() {
				@Override
				public void initialize(HttpRequest request) throws IOException {
					request.setParser(new JsonObjectParser(jsonFactory));
				}
			});

	protected static final String urlBase = "http://localhost:8983/solr/solr/";

	/*
	 * https://cwiki.apache.org/confluence/display/solr/Uploading+Data+with+Index+Handlers
	 *
	 * id - path
	 * directory_b
	 * length_l
	 * md5_s
	 */
	protected static final String updateUrl = urlBase + "update?wt=json&commit=true";

	/*
	 * simulates a task queue
	 */
	static final String selectDirectoryUrl = urlBase + "select?wt=json" +
			"&q=type_s:" + SolrDoc.DUPLICATE_CANDIDATE_TYPE +
			"&fq=directory_b:true";

	/*
	 * TODO consider the process can stop anytime, the files to be hashed may not be listed in a single query
	 */
	static final String selectDuplicateLengthUrl = urlBase + "select?wt=json" +
			"&rows=0&facet=true&facet.field=length_l&facet.mincount=2&facet.limit=-1" +
			"&q=type_s:" + SolrDoc.DUPLICATE_CANDIDATE_TYPE +
			"&fq=!directory_b:true";

	static final String selectFileWithoutMd5Url = urlBase + "select?wt=json" +
			"&q=type_s:" + SolrDoc.DUPLICATE_CANDIDATE_TYPE +
			"&fq=!directory_b:true AND !md5_s:[* TO *] AND length_l:";

	public static void main(String... args) throws IOException, NoSuchAlgorithmException {

		int pass = 0;
		do {
			List<DuplicateCandidate> update = new ArrayList<>();
			List<DuplicateCandidate> delete = new ArrayList<>();

			/*
			 * retrieve directories from solr
			 */
			List<File> directories = new ArrayList<>();
			{
				HttpRequest request = factory.buildGetRequest(new GenericUrl(selectDirectoryUrl));
				HttpResponse response = request.execute();
				DuplicateCandidate.SolrSelectResponse selectResponse =
						response.parseAs(DuplicateCandidate.SolrSelectResponse.class);
				for (DuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
					directories.add(new File(doc.getId()));
					delete.add(doc);
				}
			}
			if (directories.isEmpty()) {
				List<Long> duplicateLengths = new ArrayList<>();
				{
					/*
					 * retrieve files with same size (without hash)
					 */
					HttpRequest request = factory.buildGetRequest(new GenericUrl(selectDuplicateLengthUrl));
					HttpResponse response = request.execute();
					DuplicateCandidate.SolrSelectResponse selectResponse =
							response.parseAs(DuplicateCandidate.SolrSelectResponse.class);
					SolrFacetCounts facetCounts = selectResponse.getFacetCounts();
					SolrFacetFields facetFields = facetCounts.getFacetFields();
					Iterator<Object> iterator = facetFields.getLength().iterator();
					while (iterator.hasNext()) {
						duplicateLengths.add(Long.parseLong((String) iterator.next()));
						iterator.next();
					}
				}
				Collections.sort(duplicateLengths, Collections.reverseOrder());
				for (long length : duplicateLengths) {
					/*
					 * update hash of files with same size
					 *
					 * https://en.wikipedia.org/wiki/Secure_Hash_Algorithm
					 */
					HttpRequest request = factory.buildGetRequest(new GenericUrl(selectFileWithoutMd5Url + length));
					HttpResponse response = request.execute();
					DuplicateCandidate.SolrSelectResponse selectResponse =
							response.parseAs(DuplicateCandidate.SolrSelectResponse.class);
					for (DuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
						try {
							doc.setMd5(DigestUtils.md5Hex(new FileInputStream(doc.getId())));
							update.add(doc);
						} catch (FileNotFoundException ex) {
							delete.add(doc);
						}
					}
				}
				if (update.size() == 0 && delete.size() == 0) {
					if (pass == 0) {
						/*
						 * if there is no file to be hashed, start over again
						 */
						String home = System.getProperty("user.home");
//						directories.add(new File(home));
						directories.add(new File(home, "Desktop"));
						directories.add(new File(home, "Documents"));
						directories.add(new File(home, "Downloads"));
						directories.add(new File(home, "Music"));
						directories.add(new File(home, "Pictures"));
						directories.add(new File(home, "Public"));
						directories.add(new File(home, "Videos"));
					} else {
						break;
					}
				}
			}

			for (File d : directories) {
				File[] files = d.listFiles();
				if (files == null) {
					logger.warn("inaccessible directory: " + d.getPath());
					continue;
				}
				for (File f : files) {
					DuplicateCandidate doc = new DuplicateCandidate();

					if (f.isDirectory()) {
						doc.setDirectory(true);
					} else {
						doc.setLength(f.length());
					}

					doc.setId(f.getPath());

					update.add(doc);
				}
			}

			if (update.size() > 0) {
				HttpRequest request = factory.buildPostRequest(
						new GenericUrl(updateUrl), new JsonHttpContent(new JacksonFactory(), update));
				request.execute();
			}
			if (delete.size() > 0) {
				List<String> idList = new ArrayList<>();
				for (DuplicateCandidate c : delete) {
					idList.add(c.getId());
				}
				SolrDeleteRequest solrDeleteRequest = new SolrDeleteRequest();
				solrDeleteRequest.setDelete(idList);

				HttpRequest request = factory.buildPostRequest(
						new GenericUrl(updateUrl), new JsonHttpContent(jsonFactory, solrDeleteRequest));
				request.execute();
			}

			pass++;
		} while (true);
	}

	public static void clear() throws IOException {
		HashMap<String, Map<String, String>> data = new HashMap<>();
		HashMap<String, String> query = new HashMap<>();
		query.put("query", "type_s:" + SolrDoc.DUPLICATE_CANDIDATE_TYPE);
		data.put("delete", query);

		HttpRequest request = factory.buildPostRequest(
				new GenericUrl(updateUrl), new JsonHttpContent(jsonFactory, data));
		request.execute();
	}
}
