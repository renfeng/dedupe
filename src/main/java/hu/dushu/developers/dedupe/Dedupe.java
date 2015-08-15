package hu.dushu.developers.dedupe;

import com.google.api.client.http.*;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.JsonGenerator;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by renfeng on 8/15/15.
 */
public class Dedupe {

	private static final Logger logger = LoggerFactory
			.getLogger(Dedupe.class);

	static JacksonFactory jsonFactory = new JacksonFactory();
	static HttpTransport transport = new NetHttpTransport();

	static String urlBase = "http://localhost:8983/solr/collection1/";

	/*
	 * id - path
	 * directory_b
	 * length_i
	 * md5_s
	 */
	static String updateUrl = urlBase + "update?wt=json&commit=true";
	static String selectDirectoryUrl = urlBase + "select?wt=json" +
			"&q=directory_b:true";
	static String selectDuplicateLengthUrl = urlBase + "select?wt=json" +
			"&rows=0&facet=true&facet.field=length_i&facet.mincount=2" +
			"&q=!directory_b:true AND !md5_s:[* TO *]";
	static String selectFileWithoutMd5Url = urlBase + "select?wt=json" +
			"&q=!directory_b:true AND !md5_s:[* TO *] AND length_i:";

	public static void main(String... args) throws IOException, NoSuchAlgorithmException {

		HttpRequestFactory factory = transport.createRequestFactory(new HttpRequestInitializer() {
			@Override
			public void initialize(HttpRequest request) throws IOException {
				request.setParser(new JsonObjectParser(new JacksonFactory()));
			}
		});

		boolean done = false;
		int pass = 0;
		do {
			List<DuplicateCandicate> update = new ArrayList<>();
			List<DuplicateCandicate> delete = new ArrayList<>();

			/*
			 * TODO retrieve directories from solr
			 */
			List<File> directories = new ArrayList<>();
			{
				HttpRequest request = factory.buildGetRequest(new GenericUrl(selectDirectoryUrl));
				HttpResponse response = request.execute();
				SolrSelectResponse selectResponse = response.parseAs(SolrSelectResponse.class);
				for (DuplicateCandicate doc : selectResponse.getResponse().getDocs()) {
					directories.add(new File(doc.getId()));
					delete.add(doc);
				}
			}
			if (directories.isEmpty()) {
				List<Integer> duplicateLengths = new ArrayList<>();
				{
				/*
				 * retrieve files with same size (without hash)
				 */
					HttpRequest request = factory.buildGetRequest(new GenericUrl(selectDuplicateLengthUrl));
					HttpResponse response = request.execute();
					SolrSelectResponse selectResponse = response.parseAs(SolrSelectResponse.class);
					SolrFacetCounts facetCounts = selectResponse.getFacetCounts();
					SolrFacetFields facetFields = facetCounts.getFacetFields();
					Iterator<Object> iterator = facetFields.getLength().iterator();
					while (iterator.hasNext()) {
						duplicateLengths.add(Integer.parseInt((String) iterator.next()));
						iterator.next();
					}
				}
				for (int length : duplicateLengths) {
				/*
				 * update hash of files with same size
				 *
				 * https://en.wikipedia.org/wiki/Secure_Hash_Algorithm
				 */
					HttpRequest request = factory.buildGetRequest(new GenericUrl(selectFileWithoutMd5Url + length));
					HttpResponse response = request.execute();
					SolrSelectResponse selectResponse = response.parseAs(SolrSelectResponse.class);
					for (DuplicateCandicate doc : selectResponse.getResponse().getDocs()) {
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
						directories.add(new File(home));
					} else {
						done = true;
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
					DuplicateCandicate data = new DuplicateCandicate();

					if (f.isDirectory()) {
						data.setDirectory(true);
					} else {
						data.setLength(f.length());
					}

					data.setId(f.getPath());

					update.add(data);
				}
			}

			if (update.size() > 0) {
				HttpRequest request = factory.buildPostRequest(
						new GenericUrl(updateUrl), new JsonHttpContent(new JacksonFactory(), update));
				request.execute();
			}
			if (delete.size() > 0) {
				List<String> idList = new ArrayList<>();
				for (DuplicateCandicate c : delete) {
					idList.add(c.getId());
				}
				SolrDelete solrDelete = new SolrDelete();
				solrDelete.setDelete(idList);

				HttpRequest request = factory.buildPostRequest(
						new GenericUrl(updateUrl), new JsonHttpContent(jsonFactory, solrDelete));
				request.execute();
			}

			pass++;
		} while (!done);
	}
}
