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
import java.util.*;

/**
 * Created by renfeng on 8/15/15.
 */
public class Dedupe {

	private static final Logger logger = LoggerFactory.getLogger(Dedupe.class);

	protected final JacksonFactory jsonFactory = new JacksonFactory();

	protected final HttpRequestFactory factory = new NetHttpTransport().createRequestFactory(
			new HttpRequestInitializer() {
				@Override
				public void initialize(HttpRequest request) throws IOException {
					request.setParser(new JsonObjectParser(jsonFactory));
				}
			});

	/*
	 * wget http://archive.apache.org/dist/lucene/solr/4.10.4/solr-4.10.4-src.tgz
	 * tar xf solr-4.10.4-src.tgz
	 * cd solr-4.10.4/solr
	 * ant ivy-bootstrap example
	 * cd example
	 * java -Dsolr.solr.home=example-DIH/solr -jar start.jar
	 */
	protected final String urlBase = "http://localhost:8983/solr/solr/";

	/*
	 * https://cwiki.apache.org/confluence/display/solr/Uploading+Data+with+Index+Handlers
	 *
	 * id - path
	 * directory_b
	 * length_l
	 * md5_s
	 */
	protected final String updateUrl = urlBase + "update?wt=json&commit=true";

	/*
	 * simulates a task queue
	 */
	final String selectDirectoryUrl = urlBase + "select?indent=true&wt=json" +
			"&q=type_s:" + SolrDocBase.DUPLICATE_CANDIDATE_TYPE +
			"&fq=directory_b:true";

	/*
	 * TODO consider the process can stop anytime, the files to be hashed may not be listed in a single query
	 */
	final String selectDuplicateLengthUrl = urlBase + "select?indent=true&wt=json" +
			"&rows=0&facet=true&facet.field=length_l&facet.mincount=2&facet.limit=-1" +
			"&q=type_s:" + SolrDocBase.DUPLICATE_CANDIDATE_TYPE +
			"&fq=!directory_b:true";

	final String selectFileWithoutMd5Url = urlBase + "select?indent=true&wt=json" +
			"&q=type_s:" + SolrDocBase.DUPLICATE_CANDIDATE_TYPE +
			"&fq=!directory_b:true AND !md5_s:[* TO *] AND length_l:";

	void refresh() throws IOException {

		Queue<Long> duplicateLengthQueue = null;

		int pass = 0;
		do {
			List<DuplicateCandidate> update = new ArrayList<>();
			List<DuplicateCandidate> delete = new ArrayList<>();

			if (duplicateLengthQueue != null) {
				Long length = duplicateLengthQueue.poll();
				if (length == null) {
					break;
				}

				/*
				 * update hash of files with same size
				 *
				 * https://en.wikipedia.org/wiki/Secure_Hash_Algorithm
				 */
				String url = selectFileWithoutMd5Url + length;
				logger.info("listing duplicate files, {}", url);

				HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
				HttpResponse response = request.execute();
				DuplicateCandidate.SolrSelectResponse selectResponse =
						response.parseAs(DuplicateCandidate.SolrSelectResponse.class);
				for (DuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
					try {
						doc.setMd5(DigestUtils.md5Hex(new FileInputStream(doc.getId())));
						update.add(doc);
						logger.info("will update file: " + doc);
					} catch (FileNotFoundException ex) {
						delete.add(doc);
						logger.info("will remove non-existing file:" + doc);
					}
				}
			} else {
				/*
				 * retrieve directories from solr
				 */
				List<File> directories = new ArrayList<>();
				{
					String url = selectDirectoryUrl;
					logger.info("picking up directories, {}", url);

					HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
					HttpResponse response = request.execute();
					DuplicateCandidate.SolrSelectResponse selectResponse =
							response.parseAs(DuplicateCandidate.SolrSelectResponse.class);
					for (DuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
						String path = doc.getId();
						logger.info(path);
						directories.add(new File(path));
						delete.add(doc);
					}
				}
				if (directories.isEmpty()) {
					if (pass == 0) {
						/*
						 * if there is no file to be hashed, start over again
						 */
						String home = System.getProperty("user.home");
						directories.add(new File(home, "Desktop"));
						directories.add(new File(home, "Documents"));
						directories.add(new File(home, "Downloads"));
						directories.add(new File(home, "Music"));
						directories.add(new File(home, "Pictures"));
						directories.add(new File(home, "Public"));
						directories.add(new File(home, "Videos"));
					} else {
						/*
						 * retrieve files with same size (without hash)
						 */
						String url = selectDuplicateLengthUrl;
						logger.info("listing duplicate length, {}", url);

						HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
						HttpResponse response = request.execute();
						DuplicateCandidate.SolrSelectResponse selectResponse =
								response.parseAs(DuplicateCandidate.SolrSelectResponse.class);
						GenericSolrFacetCounts<DedupeFacetFields> facetCounts = selectResponse.getFacetCounts();
						DedupeFacetFields facetFields = facetCounts.getFacetFields();

						int lengths = facetFields.getLength().size() / 2;
						duplicateLengthQueue = Collections.asLifoQueue(new ArrayDeque<Long>(lengths));
						logger.info("different lengths to be inspected: " + lengths);

						Iterator<Object> iterator = facetFields.getLength().iterator();
						while (iterator.hasNext()) {
							duplicateLengthQueue.offer(Long.parseLong((String) iterator.next()));
							iterator.next();
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

	public void clear() throws IOException {
		HashMap<String, Map<String, String>> data = new HashMap<>();
		HashMap<String, String> query = new HashMap<>();
		query.put("query", "type_s:" + SolrDocBase.DUPLICATE_CANDIDATE_TYPE);
		data.put("delete", query);

		HttpRequest request = factory.buildPostRequest(
				new GenericUrl(updateUrl), new JsonHttpContent(jsonFactory, data));
		request.execute();
	}

	/**
	 * http://grepcode.com/file_/repo1.maven.org/maven2/org.apache.solr/solr-solrj/4.10.3/org/apache/solr/client/solrj/util/ClientUtils.java/?v=source
	 * <p/>
	 * for more information on Escaping Special Characters
	 * http://lucene.apache.org/core/4_0_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#Escaping_Special_Characters
	 */
	public String escapeQueryChars(String s) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < s.length(); i++) {
			char c = s.charAt(i);
			// These characters are part of the query syntax and must be escaped
			if (c == '\\' || c == '+' || c == '-' || c == '!' || c == '(' || c == ')' || c == ':'
					|| c == '^' || c == '[' || c == ']' || c == '\"' || c == '{' || c == '}' || c == '~'
					|| c == '*' || c == '?' || c == '|' || c == '&' || c == ';' || c == '/'
					|| Character.isWhitespace(c)) {
				sb.append('\\');
			}
			sb.append(c);
		}
		return sb.toString();
	}
}
