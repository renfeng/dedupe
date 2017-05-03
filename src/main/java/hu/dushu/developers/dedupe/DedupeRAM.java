package hu.dushu.developers.dedupe;

import com.google.common.collect.Collections2;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
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

	public void refresh(File... args) throws IOException {

		Queue<String> dirQueue = new ArrayDeque<>();
		List<DuplicateCandidate> update = new ArrayList<>();
		List<DuplicateCandidate> delete = new ArrayList<>();

		for (File p : args) {
			if (p.isDirectory()) {
				dirQueue.add(p.getPath());
			} else if (p.isFile()) {
				DuplicateCandidate candidate = new DuplicateCandidate();
				candidate.setId(p.getPath());
				candidate.setLength(p.length());
				update.add(candidate);
			} else {
				logger.warn("invalid argument: " + p);
			}
		}

		logger.info("resuming work");
		Queue<Long> duplicateLengthQueue = enqueueDuplicateLength();

		long end = System.currentTimeMillis() + INTERVAL;
		do {
			Long length = duplicateLengthQueue.poll();
			if (length != null) {
				/*
				 * update hash of files with same size
				 *
				 * https://en.wikipedia.org/wiki/Secure_Hash_Algorithm
				 */
				String url = selectFileWithoutMd5Url(length);
				logger.debug("listing duplicate files, {}", url);

				Request request = new Request.Builder().url(url).build();
				Response response = client.newCall(request).execute();
				if (!response.isSuccessful()) {
					throw new IOException("Unexpected code " + response);
				}

				DuplicateCandidate.SolrSelectResponse selectResponse =
						gson.fromJson(response.body().charStream(), DuplicateCandidate.SolrSelectResponse.class);
				response.body().close();

				for (DuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
					try {
						doc.setMd5(DigestUtils.md5Hex(new FileInputStream(doc.getId())));
						update.add(doc);
						logger.debug("will update file: " + doc);
					} catch (FileNotFoundException ex) {
						delete.add(doc);
						logger.debug("will remove non-existing file:" + doc);
					}
				}
			} else {
				String dir = dirQueue.poll();
				if (dir == null) {
					/*
					 * retrieve directories from solr
					 */
					String url = selectDirectoryUrl;
					logger.info("picking up directories, {}", url);

					Request request = new Request.Builder().url(url).build();
					Response response = client.newCall(request).execute();
					if (!response.isSuccessful()) {
						throw new IOException("Unexpected code " + response);
					}

					DuplicateCandidate.SolrSelectResponse selectResponse = gson.fromJson(
							response.body().charStream(), DuplicateCandidate.SolrSelectResponse.class);
					response.body().close();

					for (DuplicateCandidate doc : selectResponse.getResponse().getDocs()) {
						String path = doc.getId();
						dirQueue.add(path);
						delete.add(doc);
					}

					dir = dirQueue.poll();
					if (dir == null) {
						duplicateLengthQueue = enqueueDuplicateLength();
						if (duplicateLengthQueue.isEmpty()) {
							/*
							 * Mission complete!
							 */
							break;
						}
					}
				}

				File[] paths = new File(dir).listFiles();
				if (paths == null) {
					logger.warn("inaccessible directory: " + dir);
					continue;
				}
				for (File p : paths) {
					if (p.isDirectory()) {
						dirQueue.add(p.getPath());
					} else if (p.isFile()) {
						DuplicateCandidate candidate = new DuplicateCandidate();
						candidate.setId(p.getPath());
						candidate.setLength(p.length());
						update.add(candidate);
					}
				}
			}

			if (System.currentTimeMillis() > end) {
				logger.info("save dir queue and file queue");
				String path = dirQueue.poll();
				while (path != null) {
					File p = new File(path);
					DuplicateCandidate candidate = new DuplicateCandidate();
					candidate.setId(p.getPath());
					candidate.setDirectory(true);
					update.add(candidate);

					path = dirQueue.poll();
				}

				if (update.size() > 0) {
					RequestBody body = RequestBody.create(media, gson.toJson(update));
					Request request = new Request.Builder().url(updateUrl).post(body).build();
					Response response = client.newCall(request).execute();
					if (!response.isSuccessful()) {
						throw new IOException("Unexpected code " + response);
					}
				}
				if (delete.size() > 0) {
					List<String> idList = new ArrayList<>(Collections2.transform(delete, input -> input.getId()));
					SolrDeleteRequest solrDeleteRequest = new SolrDeleteRequest();
					solrDeleteRequest.setDelete(idList);

					RequestBody body = RequestBody.create(media, gson.toJson(solrDeleteRequest));
					Request request = new Request.Builder().url(updateUrl).post(body).build();
					Response response = client.newCall(request).execute();
					if (!response.isSuccessful()) {
						throw new IOException("Unexpected code " + response);
					}
				}

				end += INTERVAL;
			}
		} while (true);
	}
}
