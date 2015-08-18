package hu.dushu.developers.dedupe.jar;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.jackson2.JacksonFactory;
import hu.dushu.developers.dedupe.SolrDoc;
import hu.dushu.developers.dedupe.SolrFacetCounts;
import hu.dushu.developers.dedupe.SolrFacetFields;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.zip.ZipException;

/**
 * Created by renfeng on 8/15/15.
 */
public class DedupeJarRAM extends DedupeJar {

	/**
	 * -Djava.util.logging.config.file=logging.properties
	 * http://docs.oracle.com/javase/7/docs/api/java/util/logging/LogManager.html
	 * <p/>
	 * http://www.slf4j.org/api/org/slf4j/bridge/SLF4JBridgeHandler.html
	 * FINEST  -> TRACE
	 * FINER   -> DEBUG
	 * FINE    -> DEBUG
	 * INFO    -> INFO
	 * WARNING -> WARN
	 * SEVERE  -> ERROR
	 */
	static final Logger logger = LoggerFactory.getLogger(DedupeJarRAM.class);

	static final String selectJarWithoutTagUrl = urlBase + "select?wt=json" +
			"&rows=0&facet=true&facet.field=jar_s&facet.mincount=1&facet.limit=-1" +
			"&q=type_s:" + SolrDoc.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE +
			"&fq=!tag_ss:%1$s";

	static final String selectJarEntryUrl = urlBase + "select?wt=json" +
			"&rows=%1$s&q=type_s:" + SolrDoc.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE +
			"&fq=jar_s:%2$s";

	static final String selectDuplicateJarEntryUrl = urlBase + "select?wt=json" +
			"&q=type_s:" + SolrDoc.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE +
			"&fq=entry_s:%1$s+AND+md5_s:%2$s";

	/*
	 * for debug purpose
	 *
	 * list duplicate md5
	 * http://slc01bfk/solr/solr/select?wt=json&rows=0&facet=true&facet.sort=count&facet.field=md5_s&facet.limit=-1&q=type_s:hu.dushu.developers.dedupe.jar.JarResourceDuplicateCandidate
	 *
	 * list jar entries of given md5
	 * http://slc01bfk/solr/solr/select?wt=json&q=type_s:hu.dushu.developers.dedupe.jar.JarResourceDuplicateCandidate%20AND%20md5_s:3b83ef96387f14655fc854ddc3c6bd57
	 *
	 * list jar entries with duplicates
	 * http://slc01bfk/solr/solr/select?wt=json&q=type_s:hu.dushu.developers.dedupe.jar.JarResourceDuplicateCandidate%20AND%20md5_s:[*%20TO%20*]
	 */

	public static void main(String... args) throws IOException, NoSuchAlgorithmException {

		clear();

		/*
		 * TODO incremental
		 */
		refresh();

		Map<String, Integer> map = listJarsWithoutTag("approved");
		Iterator<Map.Entry<String, Integer>> iterator = map.entrySet().iterator();
		if (iterator.hasNext()) {
			Map.Entry<String, Integer> entry = iterator.next();
			tag(entry.getKey(), entry.getValue(), "approved");
		}
	}

	private static Map<String, Integer> listJarsWithoutTag(String tag) throws IOException {

		/*
		 * list all jars
		 * http://localhost:8983/solr/solr/select?wt=json&rows=0&facet=true&facet.field=jar_s&facet.limit=-1&q=type_s:hu.dushu.developers.dedupe.jar.JarResourceDuplicateCandidate
		 */

		Map<String, Integer> map = new HashMap<>();

		String url = String.format(selectJarWithoutTagUrl, URLEncoder.encode(tag, "UTF-8"));
		logger.info("listing jars without tag, {}", url);

		HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
		HttpResponse response = request.execute();
		JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
				response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
		SolrFacetCounts facetCounts = selectResponse.getFacetCounts();
		SolrFacetFields facetFields = facetCounts.getFacetFields();
		Iterator<Object> iterator = facetFields.getJar().iterator();
		while (iterator.hasNext()) {
			String jar = (String) iterator.next();
			BigDecimal count = (BigDecimal) iterator.next();
			map.put(jar, count.intValue());
		}

		return map;
	}

	private static void tag(String jar, int entries, String... tags) throws IOException {

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

			String url = String.format(selectJarEntryUrl, entries, URLEncoder.encode(
					"\"" + escapeQueryChars(jar) + "\"", "UTF-8"));
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

	private static void tagJarEntry(
			List<JarEntryDuplicateCandidate> update, String jarEntry, String md5, List<String> tags)
			throws IOException {

		String url = String.format(selectDuplicateJarEntryUrl, jarEntry, md5);
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

	private static void refresh() throws IOException {

		Queue<File> directoryQueue = Collections.asLifoQueue(new LinkedList<File>());
		Queue<File> jarQueue = Collections.asLifoQueue(new LinkedList<File>());
		List<JarEntryDuplicateCandidate> duplicateCandidates = new LinkedList<>();

		String home = System.getProperty("user.home");
		directoryQueue.offer(new File(home, ".m2/repository/hu"));
//		directoryQueue.offer(new File("Z:/"));

		File d = directoryQueue.poll();
		while (d != null) {
			File[] files = d.listFiles();
			if (files != null) {
				for (File f : files) {
					if (f.isDirectory()) {
						directoryQueue.offer(f);
//						logger.trace("discovered directory: " + f.getPath());
					} else if (f.getName().endsWith(".jar")) {
						jarQueue.offer(f);
//						logger.debug("discovered jar file: " + f.getPath());
					}
				}
			} else {
				logger.warn("inaccessible directory: " + d.getPath());
			}
			d = directoryQueue.poll();
		}

		logger.info("jar files queued: " + jarQueue.size());

		File j = jarQueue.poll();
		while (j != null) {
			String path = j.getPath();

			/*
			 * http://download.java.net/jdk7/archive/b123/docs/api/java/net/JarURLConnection.html
			 */
			URL url = new URL("jar:file:" + path + "!/");
			JarURLConnection connection = (JarURLConnection) url.openConnection();
			try {
				JarFile jarFile = connection.getJarFile();
				Enumeration<JarEntry> entries = jarFile.entries();
				while (entries.hasMoreElements()) {
					JarEntry jarEntry = entries.nextElement();

					if (jarEntry.isDirectory()) {
						continue;
					}

					String entry = jarEntry.getName();

					JarEntryDuplicateCandidate candidate = new JarEntryDuplicateCandidate();
					candidate.setId("jar:file:" + path + "!/" + entry);
					candidate.setLength(jarEntry.getSize());
					candidate.setJar(path);
					candidate.setEntry(entry);

					int i = Collections.binarySearch(duplicateCandidates, candidate);
					if (i < 0) {
						i = -1 - i;
					}
					duplicateCandidates.add(i, candidate);
//					logger.debug("discovered jar entry file: " + candidate.getId());
				}
			} catch (ZipException ex) {
				logger.info("non-zip file skipped: " + path, ex);
			}

			j = jarQueue.poll();
		}

		logger.info("jar entries queued: " + duplicateCandidates.size());

		if (duplicateCandidates.size() > 0) {
			JarEntryDuplicateCandidate candidate = duplicateCandidates.get(0);
			boolean hasDuplicate = false;
			for (int i = 1; i < duplicateCandidates.size(); i++) {
				JarEntryDuplicateCandidate nextCandidate = duplicateCandidates.get(i);
				if (nextCandidate.compareTo(candidate) == 0) {
					updateMD5(candidate);
					hasDuplicate = true;
				} else if (hasDuplicate) {
					updateMD5(candidate);
					hasDuplicate = false;
				}
				candidate = nextCandidate;
			}
			if (hasDuplicate) {
				updateMD5(candidate);
			}

			HttpRequest request = factory.buildPostRequest(
					new GenericUrl(updateUrl), new JsonHttpContent(new JacksonFactory(), duplicateCandidates));
			request.execute();
		}
	}

	private static void updateMD5(JarEntryDuplicateCandidate candidate) throws IOException {
		URL url = new URL(candidate.getId());
		JarURLConnection connection = (JarURLConnection) url.openConnection();
		candidate.setMd5(DigestUtils.md5Hex(connection.getInputStream()));
		logger.debug("updated md5 for duplicate jar entry: " + candidate.getId());
	}

	/**
	 * http://grepcode.com/file_/repo1.maven.org/maven2/org.apache.solr/solr-solrj/4.10.3/org/apache/solr/client/solrj/util/ClientUtils.java/?v=source
	 * <p/>
	 * for more information on Escaping Special Characters
	 * http://lucene.apache.org/core/4_0_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#Escaping_Special_Characters
	 */
	public static String escapeQueryChars(String s) {
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
