package hu.dushu.developers.dedupe.jar;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.jackson2.JacksonFactory;
import hu.dushu.developers.dedupe.Dedupe;
import hu.dushu.developers.dedupe.GenericSolrFacetCounts;
import hu.dushu.developers.dedupe.SolrDeleteRequest;
import hu.dushu.developers.dedupe.SolrDocBase;
import org.apache.commons.codec.EncoderException;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.zip.ZipException;

/**
 * Created by renfeng on 8/15/15.
 */
public class DedupeJar extends Dedupe {

	static final Logger logger = LoggerFactory.getLogger(DedupeJar.class);

	String encodeQueryValue(String value) throws EncoderException {
//		return URLEncoder.encode(value, "UTF-8");
//		return new URLCodec().encode(value);
		return value;
	}

	/*
	 * simulates a task queue to traverse through directories
	 */
	String selectDirectoryUrl() throws EncoderException {
		return urlBase + "select?indent=true&wt=json" +
				"&q=" + encodeQueryValue("type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE) +
				"&fq=" + encodeQueryValue("directory_b:true");
	}

	String selectDuplicateJarEntryUrl(String jarEntry, String md5) throws EncoderException {
		return urlBase + "select?indent=true&wt=json" +
				"&q=" + encodeQueryValue("type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE) +
				"&fq=" + encodeQueryValue(String.format("entry_s:%1$s AND md5_s:%2$s", jarEntry, md5));
	}

	String selectJarEntryUrl(String jar, int count) throws EncoderException {
		return urlBase + "select?indent=true&wt=json" +
				"&rows=" + count +
				"&q=" + encodeQueryValue("type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE) +
				"&fq=" + encodeQueryValue("jar_s:\"" + escapeQueryChars(jar) + "\"");
	}

	String selectJarWithoutTagUrl(String tag) throws EncoderException {
		return urlBase + "select?indent=true&wt=json" +
				"&rows=0&facet=true&facet.mincount=1&facet.limit=-1&facet.field=jar_s" +
				"&q=" + encodeQueryValue("type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE) +
				"&fq=" + encodeQueryValue("!tag_ss:" + tag);
	}

	private String selectJarEntryWithoutMd5Url(long length) throws EncoderException {
		String fq = "!directory_b:true AND jar_s:[* TO *] AND length_l:" + length;
		return urlBase + "select?indent=true&wt=json" +
				"&rows=" + Integer.MAX_VALUE +
				"&q=" + encodeQueryValue("type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE) +
				"&fq=" + encodeQueryValue(fq);
	}

	private String selectJarEntriesBySpecification(String title, String version) throws EncoderException {
		String fq;
		if (version != null) {
			fq = String.format("!directory_b:true AND !jar_s:[* TO *] AND " +
							"specificationTitle_s:\"%1$s\" AND specificationVersion_s:\"%2$s\"",
					escapeQueryChars(title), escapeQueryChars(version));
		} else {
			fq = String.format("!directory_b:true AND !jar_s:[* TO *] AND " +
					"specificationTitle_s:\"%1$s\" AND " +
					"!specificationVersion_s:[* TO *]", escapeQueryChars(title));
		}
		return urlBase + "select?indent=true&wt=json" +
				"&rows=0&facet=true&facet.mincount=1&facet.limit=-1&facet.field=id" +
				"&q=" + encodeQueryValue("type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE) +
				"&fq=" + encodeQueryValue(fq);
	}

	private String selectJarSpecificationsWithoutTag(String tag) throws EncoderException {
		String fq = String.format("!tag_s:%1$s", tag);
		return urlBase + "select?indent=true&wt=json" +
				"&rows=0&facet=true&facet.mincount=1&facet.limit=-1&facet.field=specificationTitle_s" +
				"&q=" + encodeQueryValue("type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE) +
				"&fq=" + encodeQueryValue(fq);
	}

	private String selectJarSpecificationVersions(String title) throws EncoderException {
		String fq = String.format("specificationTitle_s:\"%1$s\"", escapeQueryChars(title));
		return urlBase + "select?indent=true&wt=json" +
				"&rows=0&facet=true&facet.mincount=1&facet.limit=-1&facet.field=specificationVersion_s" +
				"&q=" + encodeQueryValue("type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE) +
				"&fq=" + encodeQueryValue(fq);
	}

	public void clear() throws IOException {
		HashMap<String, Map<String, String>> data = new HashMap<>();
		HashMap<String, String> query = new HashMap<>();
		query.put("query", "type_s:" + SolrDocBase.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE);
		data.put("delete", query);

		HttpRequest request = factory.buildPostRequest(
				new GenericUrl(updateUrl), new JsonHttpContent(jsonFactory, data));
		request.execute();
	}

	private void init(List<File> directories) {
//		String home = System.getProperty("user.home");
//		directories.add(new File(home, ".m2/repository/commons-beanutils"));
//		directories.add(new File(home, ".m2/repository/commons-chain"));
//		directories.add(new File(home, ".m2/repository/commons-cli"));
//		directories.add(new File(home, ".m2/repository/commons-codec"));
//		directories.add(new File(home, ".m2/repository/commons-collections"));
//		directories.add(new File(home, ".m2/repository/commons-digester"));
//		directories.add(new File(home, ".m2/repository/commons-httpclient"));
//		directories.add(new File(home, ".m2/repository/commons-io"));
//		directories.add(new File(home, ".m2/repository/commons-lang"));
//		directories.add(new File(home, ".m2/repository/commons-logging"));
//		directories.add(new File(home, ".m2/repository/commons-validator"));
		directories.add(new File("Z:/"));
	}

	public void refresh() throws IOException, EncoderException {

		int pass = 0;
		do {
			List<JarEntryDuplicateCandidate> update = new ArrayList<>();
			List<JarEntryDuplicateCandidate> delete = new ArrayList<>();

			/*
			 * retrieve directories from solr
			 */
			List<File> directories = new ArrayList<>();
			{
				String url = selectDirectoryUrl();
				logger.info("picking up directories, {}", url);

				HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
				HttpResponse response = request.execute();
				JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
						response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
				for (JarEntryDuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
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
					init(directories);
				} else {
					break;
				}
			}

			List<File> jars = new ArrayList<>();

			for (File d : directories) {
				File[] files = d.listFiles();
				if (files == null) {
					logger.warn("inaccessible directory: " + d.getPath());
					continue;
				}
				for (File f : files) {
					if (f.isDirectory()) {
						JarEntryDuplicateCandidate doc = new JarEntryDuplicateCandidate();
						doc.setId(f.getPath());
						doc.setDirectory(true);
						update.add(doc);
					} else if (f.getName().endsWith(".jar")) {
						jars.add(f);
					}
				}
			}

			/*
			 * process jar files
			 */
			for (File j : jars) {
				String path = j.getPath();
				long length = j.length();

				/*
				 * add jar files to solr for there may be some jar is invalid, and we need to know that
				 * by searching for jars (!directory_b:true AND !jar_s:[* TO *]) without an entry
				 */
				JarEntryDuplicateCandidate doc = new JarEntryDuplicateCandidate();
				doc.setId(path);
				doc.setLength(length);
				update.add(doc);

				if (length > 0) {
					/*
					 * http://download.java.net/jdk7/archive/b123/docs/api/java/net/JarURLConnection.html
					 */
					URL url = new URL("jar:file:" + path + "!/");
					JarURLConnection connection = (JarURLConnection) url.openConnection();
					try {
						Manifest manifest = connection.getManifest();
						if (manifest != null) {
							Attributes mainAttributes = manifest.getMainAttributes();
							doc.setSpecificationTitle(mainAttributes.getValue("Specification-Title"));
							doc.setSpecificationVender(mainAttributes.getValue("Specification-Vender"));
							doc.setSpecificationVersion(mainAttributes.getValue("Specification-Version"));
						}

						JarFile jarFile = connection.getJarFile();
						Enumeration<JarEntry> entries = jarFile.entries();
						while (entries.hasMoreElements()) {
							JarEntry jarEntry = entries.nextElement();

							if (jarEntry.isDirectory()) {
								continue;
							}

							String entry = jarEntry.getName();
							long size = jarEntry.getSize();

							int entriesOfSameSize = updateMd5(update, delete, size);

							JarEntryDuplicateCandidate entryDoc = new JarEntryDuplicateCandidate();
							String id = "jar:file:" + path + "!/" + entry;
							entryDoc.setId(id);
							entryDoc.setLength(size);
							entryDoc.setJar(path);
							entryDoc.setEntry(entry);
							if (entriesOfSameSize > 0) {
								URL entryUrl = new URL(id);
								JarURLConnection entryConnection = (JarURLConnection) entryUrl.openConnection();
								entryDoc.setMd5(DigestUtils.md5Hex(entryConnection.getInputStream()));
							}
							update.add(entryDoc);
						}
					} catch (ZipException ex) {
						logger.info("non-zip file skipped: " + path, ex);
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
				for (JarEntryDuplicateCandidate c : delete) {
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

	private int updateMd5(
			List<JarEntryDuplicateCandidate> update, List<JarEntryDuplicateCandidate> delete, Long length)
			throws IOException, EncoderException {

		/*
		 * update hash of files with same size
		 *
		 * https://en.wikipedia.org/wiki/Secure_Hash_Algorithm
		 */
		String url = selectJarEntryWithoutMd5Url(length);
		logger.info("listing duplicate candidate jar entries, {}", url);

		HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
		HttpResponse response = request.execute();
		JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
				response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
		for (JarEntryDuplicateCandidate c : selectResponse.getResponse().getDocs()) {
			if (c.getMd5() != null) {
				continue;
			}
			try {
				/*
				 * http://download.java.net/jdk7/archive/b123/docs/api/java/net/JarURLConnection.html
				 */
				JarURLConnection connection = (JarURLConnection) new URL(c.getId()).openConnection();
				c.setMd5(DigestUtils.md5Hex(connection.getInputStream()));
				update.add(c);
			} catch (FileNotFoundException ex) {
				delete.add(c);
			}
		}

		return selectResponse.getResponse().getNumFound();
	}

	public void tag(String jar, int count, String... tags) throws IOException, EncoderException {

		/*
		 * see if a jar has a tag
		 * http://localhost:8983/solr/solr/select?wt=json&q=type_s:hu.dushu.developers.dedupe.jar.JarResourceDuplicateCandidate%20AND%20jar_s:%22C:\\Users\\frren\\.m2\\repository\\commons-io\\commons-io\\2.4\\commons-io-2.4.jar%22%20AND%20tag_ss:approved
		 *
		 * or not
		 * http://localhost:8983/solr/solr/select?wt=json&q=type_s:hu.dushu.developers.dedupe.jar.JarResourceDuplicateCandidate%20AND%20jar_s:%22C:\\Users\\frren\\.m2\\repository\\commons-io\\commons-io\\2.4\\commons-io-2.4.jar%22%20AND%20!tag_ss:approved
		 *
		 * tag a jar is a process of listing jar entries, for each entry listing identical ones without the given tag, adding the tag, and updating them all
		 */

		logger.info("tagging jar file, {}, with tags {}", jar, tags);

		List<JarEntryDuplicateCandidate> update = new ArrayList<>();

		{
			List<String> tagList = Arrays.asList(tags);

			String url = selectJarEntryUrl(jar, count);
			logger.info("listing jar entries, {}", url);

			HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
			HttpResponse response = request.execute();
			JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
					response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
			for (JarEntryDuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
				String md5 = doc.getMd5();
				String jarEntry = doc.getEntry();
				if (md5 != null) {
					logger.info("tagging duplicate jar entries, {} ({}), with tags {}", jarEntry, md5, tags);
					tagJarEntry(update, jarEntry, md5, tagList);
				} else {
					doc.setTags(tagList);
//					logger.info("tagging unique jar entry, {}, with tags {}", jarEntry, tags);
					update.add(doc);
				}
			}
		}

		{
			HttpRequest request = factory.buildPostRequest(
					new GenericUrl(updateUrl), new JsonHttpContent(new JacksonFactory(), update));
			request.execute();
		}
	}

	private void tagJarEntry(
			List<JarEntryDuplicateCandidate> update, String jarEntry, String md5, List<String> tags)
			throws IOException, EncoderException {

		String url = selectDuplicateJarEntryUrl(jarEntry, md5);
		logger.info("listing duplicate jar entries, {}", url);

		HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
		HttpResponse response = request.execute();
		JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
				response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
		for (JarEntryDuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
			doc.setTags(tags);
			update.add(doc);
		}
	}

	protected SortedMap<String, Integer> listJarsWithoutTag(String tag) throws IOException, EncoderException {

		/*
		 * list all jars
		 * http://localhost:8983/solr/solr/select?wt=json&rows=0&facet=true&facet.field=jar_s&facet.limit=-1&q=type_s:hu.dushu.developers.dedupe.jar.JarResourceDuplicateCandidate
		 */

		SortedMap<String, Integer> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

		String url = selectJarWithoutTagUrl(tag);
		logger.info("listing jars without tag, {}", url);

		HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
		HttpResponse response = request.execute();
		JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
				response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
		GenericSolrFacetCounts<JarDedupeFacetFields> facetCounts = selectResponse.getFacetCounts();
		JarDedupeFacetFields facetFields = facetCounts.getFacetFields();
		Iterator<Object> iterator = facetFields.getJar().iterator();
		while (iterator.hasNext()) {
			String jar = (String) iterator.next();
			BigDecimal count = (BigDecimal) iterator.next();
			map.put(jar, count.intValue());
		}

		return map;
	}

	public void tagBySpecification(String title, String version, String... tags) throws EncoderException, IOException {

		List<String> pathList = listJarsBySpecification(title, version);

		for (String path : pathList) {
			tag(path, Integer.MAX_VALUE, tags);
			logger.info("tagged jar {} with tags {}", path, tags);
		}
	}

	public List<String> listJarsBySpecification(String title, String version)
			throws EncoderException, IOException {

		List<String> pathList = new ArrayList<>();

		String url = selectJarEntriesBySpecification(title, version);
		logger.info("listing duplicate candidate jar entries, {}", url);

		HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
		HttpResponse response = request.execute();
		JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
				response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
		GenericSolrFacetCounts<JarDedupeFacetFields> facetCounts = selectResponse.getFacetCounts();
		JarDedupeFacetFields facetFields = facetCounts.getFacetFields();
		Iterator<Object> iterator = facetFields.getId().iterator();
		while (iterator.hasNext()) {
			String path = (String) iterator.next();
			BigDecimal count = (BigDecimal) iterator.next();
			pathList.add(path);
		}
		return pathList;
	}

	public SortedMap<String, Integer> listSpecificationsWithoutTag(String tag) throws EncoderException, IOException {

		SortedMap<String, Integer> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

		String url = selectJarSpecificationsWithoutTag(tag);
		logger.info("listing specification titles not tagged {}", url);

		HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
		HttpResponse response = request.execute();
		JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
				response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
		GenericSolrFacetCounts<JarDedupeFacetFields> facetCounts = selectResponse.getFacetCounts();
		JarDedupeFacetFields facetFields = facetCounts.getFacetFields();
		Iterator<Object> iterator = facetFields.getSpecificationTitle().iterator();
		while (iterator.hasNext()) {
			String spec = (String) iterator.next();
			BigDecimal count = (BigDecimal) iterator.next();
			map.put(spec, count.intValue());
		}

		return map;
	}

	public SortedMap<String, Integer> listSpecificationVersions(String title) throws IOException, EncoderException {

		SortedMap<String, Integer> map = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

		String url = selectJarSpecificationVersions(title);
		logger.info("listing specification versions, {}", url);

		HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
		HttpResponse response = request.execute();
		JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
				response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
		GenericSolrFacetCounts<JarDedupeFacetFields> facetCounts = selectResponse.getFacetCounts();
		JarDedupeFacetFields facetFields = facetCounts.getFacetFields();
		Iterator<Object> iterator = facetFields.getSpecificationVersion().iterator();
		while (iterator.hasNext()) {
			String ver = (String) iterator.next();
			BigDecimal count = (BigDecimal) iterator.next();
			map.put(ver, count.intValue());
		}

		return map;
	}
}
