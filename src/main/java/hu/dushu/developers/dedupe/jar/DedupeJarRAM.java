//package hu.dushu.developers.dedupe.jar;
//
//import com.google.api.client.http.GenericUrl;
//import com.google.api.client.http.HttpRequest;
//import com.google.api.client.http.HttpResponse;
//import com.google.api.client.http.json.JsonHttpContent;
//import com.google.api.client.json.jackson2.JacksonFactory;
//import hu.dushu.developers.dedupe.*;
//import org.apache.commons.codec.digest.DigestUtils;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.net.JarURLConnection;
//import java.net.URL;
//import java.security.NoSuchAlgorithmException;
//import java.util.*;
//import java.util.jar.JarEntry;
//import java.util.jar.JarFile;
//import java.util.zip.ZipException;
//
///**
// * Created by renfeng on 8/15/15.
// */
//public class DedupeJarRAM {
//
//	static final Logger logger = LoggerFactory.getLogger(DedupeJarRAM.class);
//
//	public static void main(String... args) throws IOException, NoSuchAlgorithmException {
//
//		Queue<File> directoryQueue = Collections.asLifoQueue(new LinkedList<File>());
//		Queue<File> jarQueue = Collections.asLifoQueue(new LinkedList<File>());
//		Queue<Long> duplicateLengthQueue = Collections.asLifoQueue(new LinkedList<Long>());
//
//		String home = System.getProperty("user.home");
//		directoryQueue.offer(new File(home, ".m2/repository"));
//
//		File d = directoryQueue.poll();
//		while (d!=null) {
//			File[] files = d.listFiles();
//			if (files == null) {
//				logger.warn("inaccessible directory: " + d.getPath());
//				continue;
//			}
//			for (File f : files) {
//				if (f.isDirectory()) {
//					directoryQueue.offer(f);
//				} else if (f.getName().endsWith(".jar")) {
//					jarQueue.offer(f);
//				}
//			}
//
//			dir = directoryQueue.poll();
//		}
//
//		int pass = 0;
//		do {
//			List<JarEntryDuplicateCandidate> update = new ArrayList<>();
//			List<JarEntryDuplicateCandidate> delete = new ArrayList<>();
//
//			/*
//			 * retrieve directories from solr
//			 */
//			List<File> directories = new ArrayList<>();
//			{
//				HttpRequest request = factory.buildGetRequest(new GenericUrl(selectDirectoryUrl));
//				HttpResponse response = request.execute();
//				JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
//						response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
//				for (JarEntryDuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
//					directories.add(new File(doc.getId()));
//					delete.add(doc);
//				}
//			}
//			if (directories.isEmpty()) {
//				/*
//				 * process jar files
//				 */
//				List<File> jars = new ArrayList<>();
//				{
//					HttpRequest request = factory.buildGetRequest(new GenericUrl(selectJarUrl));
//					HttpResponse response = request.execute();
//					JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
//							response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
//					for (JarEntryDuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
//						jars.add(new File(doc.getId()));
//						delete.add(doc);
//					}
//				}
//				if (jars.isEmpty()) {
////					List<Long> duplicateLengths = new ArrayList<>();
////					duplicatelengthQueue = Collections.asLifoQueue(new ArrayDeque<Long>());
//					{
//					/*
//					 * retrieve files with same size (without hash)
//					 */
//						HttpRequest request = factory.buildGetRequest(new GenericUrl(selectDuplicateLengthUrl));
//						HttpResponse response = request.execute();
//						JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
//								response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
//						SolrFacetCounts facetCounts = selectResponse.getFacetCounts();
//						SolrFacetFields facetFields = facetCounts.getFacetFields();
//						Iterator<Object> iterator = facetFields.getLength().iterator();
//						while (iterator.hasNext()) {
////							duplicateLengths.add(Long.parseLong((String) iterator.next()));
//							duplicateLengthQueue.offer(Long.parseLong((String) iterator.next()));
//							iterator.next();
//						}
//					}
////					Collections.sort(duplicateLengths, Collections.reverseOrder());
//					for (long length : duplicateLengths) {
//					/*
//					 * update hash of files with same size
//					 *
//					 * https://en.wikipedia.org/wiki/Secure_Hash_Algorithm
//					 */
//						HttpRequest request = factory.buildGetRequest(new GenericUrl(selectJarEntryWithoutMd5Url + length));
//						HttpResponse response = request.execute();
//						JarEntryDuplicateCandidate.SolrSelectResponse selectResponse =
//								response.parseAs(JarEntryDuplicateCandidate.SolrSelectResponse.class);
//						for (JarEntryDuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
//							try {
//								/*
//								 * http://download.java.net/jdk7/archive/b123/docs/api/java/net/JarURLConnection.html
//								 */
//								URL url = new URL(doc.getId());
//								JarURLConnection connection = (JarURLConnection) url.openConnection();
//								doc.setMd5(DigestUtils.md5Hex(connection.getInputStream()));
//								update.add(doc);
//							} catch (FileNotFoundException ex) {
//								delete.add(doc);
//							}
//						}
//					}
//				} else {
//					for (File j : jars) {
//						/*
//						 * http://download.java.net/jdk7/archive/b123/docs/api/java/net/JarURLConnection.html
//						 */
//						URL url = new URL("jar:file:" + j.getPath() + "!/");
//						JarURLConnection connection = (JarURLConnection) url.openConnection();
//						try {
//							JarFile jarFile = connection.getJarFile();
//							Enumeration<JarEntry> entries = jarFile.entries();
//							while (entries.hasMoreElements()) {
//								JarEntry jarEntry = entries.nextElement();
//
//								if (jarEntry.isDirectory()) {
//									continue;
//								}
//
//								JarEntryDuplicateCandidate doc = new JarEntryDuplicateCandidate();
//								doc.setId("jar:file:" + j.getPath() + "!/" + jarEntry.getName());
//								doc.setLength(jarEntry.getSize());
//								doc.setJar(j.getPath());
//								update.add(doc);
//							}
//						} catch (ZipException ex) {
//							logger.info("non-zip file skipped: " + j.getPath(), ex);
//						}
//					}
//				}
//				if (update.size() == 0 && delete.size() == 0) {
//					if (pass == 0) {
//						/*
//						 * if there is no file to be hashed, start over again
//						 */
//						String home = System.getProperty("user.home");
////						directories.add(new File(home));
//						directories.add(new File(home, ".m2/repository"));
//					} else {
//						break;
//					}
//				}
//			}
//
//			for (File d : directories) {
//				File[] files = d.listFiles();
//				if (files == null) {
//					logger.warn("inaccessible directory: " + d.getPath());
//					continue;
//				}
//				for (File f : files) {
//					if (f.isDirectory()) {
//						JarEntryDuplicateCandidate doc = new JarEntryDuplicateCandidate();
//						doc.setId(f.getPath());
//						doc.setDirectory(true);
//						update.add(doc);
//					} else if (f.getName().endsWith(".jar")) {
//						JarEntryDuplicateCandidate doc = new JarEntryDuplicateCandidate();
//						doc.setId(f.getPath());
//						update.add(doc);
//					}
//				}
//			}
//
//			if (update.size() > 0) {
//				HttpRequest request = factory.buildPostRequest(
//						new GenericUrl(updateUrl), new JsonHttpContent(new JacksonFactory(), update));
//				request.execute();
//			}
//			if (delete.size() > 0) {
//				List<String> idList = new ArrayList<>();
//				for (JarEntryDuplicateCandidate c : delete) {
//					idList.add(c.getId());
//				}
//				SolrDeleteRequest solrDeleteRequest = new SolrDeleteRequest();
//				solrDeleteRequest.setDelete(idList);
//
//				HttpRequest request = factory.buildPostRequest(
//						new GenericUrl(updateUrl), new JsonHttpContent(jsonFactory, solrDeleteRequest));
//				request.execute();
//			}
//
//			pass++;
//		} while (true);
//	}
//
//	public static void clear() throws IOException {
//		HashMap<String, Map<String, String>> data = new HashMap<>();
//		HashMap<String, String> query = new HashMap<>();
//		query.put("query", "type_s:" + SolrDoc.JAR_RESOURCE_DUPLICATE_CANDIDATE_TYPE);
//		data.put("delete", query);
//
//		HttpRequest request = factory.buildPostRequest(
//				new GenericUrl(updateUrl), new JsonHttpContent(jsonFactory, data));
//		request.execute();
//	}
//}
