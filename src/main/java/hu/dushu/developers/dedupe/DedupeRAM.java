package hu.dushu.developers.dedupe;

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.json.JsonHttpContent;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import work.fair24.solr.SolrDeleteRequest;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Created by renfeng on 5/3/17.
 */
public class DedupeRAM extends Dedupe {

	private static final Logger logger = LoggerFactory.getLogger(DedupeRAM.class);

	/**
	 * save work every minute
	 */
	private static final int INTERVAL = 1000 * 60;

	public void refresh(File... paths) throws IOException {

		Queue<String> dirQueue = new ArrayDeque<>();
		List<DuplicateCandidate> update = new ArrayList<>();
		List<String> delete = new ArrayList<>();

		long end = System.currentTimeMillis() + INTERVAL;
		boolean done = false;
		Queue<Long> duplicateLengthQueue = null;

		/*
		 * e.g. 1275017 success
		 * e.g. 1622196 failure
		 * e.g. 284161 failure
		 */
		int cap = Integer.MAX_VALUE;

		do {
			if (paths != null) {
				for (File p : paths) {
					if (p.isDirectory()) {
						dirQueue.add(p.getPath());
					} else if (p.isFile()) {
						DuplicateCandidate candidate = new DuplicateCandidate();
						candidate.setId(p.getPath());
						candidate.setLength(p.length());
						update.add(candidate);
					} else {
						logger.warn("invalid path: " + p);
					}
				}
			}

			String dir = dirQueue.poll();
			if (dir == null) {
				String url = selectDirectoryUrl;
				logger.info("fetching directories, {}", url);

				HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
				HttpResponse response = request.execute();
				DuplicateCandidate.SolrSelectResponse selectResponse =
						response.parseAs(DuplicateCandidate.SolrSelectResponse.class);
				for (DuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
					String path = doc.getId();
					dirQueue.add(path);
				}

				dir = dirQueue.poll();
			}

			if (dir != null) {
				paths = new File(dir).listFiles();
				delete.add(dir);
			} else {
				if (duplicateLengthQueue == null) {
					duplicateLengthQueue = enqueueDuplicateLength();
				}

				Long length = duplicateLengthQueue.poll();
				if (length != null) {
					/*
					 * update hash of files with same size
					 *
					 * https://en.wikipedia.org/wiki/Secure_Hash_Algorithm
					 */
					String url = selectFileWithoutMd5Url(length);
					logger.debug("listing duplicate files, {}", url);

					HttpRequest request = factory.buildGetRequest(new GenericUrl(url));
					HttpResponse response = request.execute();
					DuplicateCandidate.SolrSelectResponse selectResponse =
							response.parseAs(DuplicateCandidate.SolrSelectResponse.class);
					for (DuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
						try {
							doc.setMd5(DigestUtils.md5Hex(new FileInputStream(doc.getId())));
							update.add(doc);
							logger.debug("will update file: " + doc);
						} catch (FileNotFoundException ex) {
							delete.add(doc.getId());
							logger.debug("will remove non-existing file:" + doc);
						}
					}
				} else {
					/*
					 * Mission complete!
					 */
					done = true;
				}
			}

			if (done || System.currentTimeMillis() > end) {
				logger.info("saving dir queue and file queue");
				if (!dirQueue.isEmpty()) {
					String path = dirQueue.poll();
					while (path != null) {
						File p = new File(path);
						DuplicateCandidate candidate = new DuplicateCandidate();
						candidate.setId(p.getPath());
						candidate.setDirectory(true);
						update.add(candidate);

						path = dirQueue.poll();
					}
				}

				if (!update.isEmpty()) {
					logger.info("updating: {}", update.size());
					HttpRequest request = factory.buildPostRequest(
							new GenericUrl(updateUrl), new JsonHttpContent(new JacksonFactory(), update));
					request.execute();
					update.clear();
				}
				if (!delete.isEmpty()) {
					logger.info("deleting: {}", delete.size());
					SolrDeleteRequest solrDeleteRequest = new SolrDeleteRequest();
					solrDeleteRequest.setDelete(delete);

					HttpRequest request = factory.buildPostRequest(
							new GenericUrl(updateUrl), new JsonHttpContent(jsonFactory, solrDeleteRequest));
					request.execute();
					delete.clear();
				}

				if (done) {
					break;
				}
				end += INTERVAL;
			}
		} while (true);
	}
}
