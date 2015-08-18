package hu.dushu.developers.dedupe.jar;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
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

	public void refresh() throws IOException {

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
}
