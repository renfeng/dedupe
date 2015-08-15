package hu.dushu.developers.dedupe;

import com.google.api.client.util.Key;

/**
 * Created by renfeng on 8/15/15.
 */
public class DuplicateCandicate extends SolrDoc {

//	@Key("id")
//	private String path;

	@Key("directory_b")
	private boolean directory;

	@Key("length_i")
	private long length;

	@Key("md5_s")
	private String md5;

//	public String getPath() {
//		return path;
//	}
//
//	public void setPath(String path) {
//		this.path = path;
//	}

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public String getMd5() {
		return md5;
	}

	public void setMd5(String md5) {
		this.md5 = md5;
	}

	public boolean isDirectory() {
		return directory;
	}

	public void setDirectory(boolean directory) {
		this.directory = directory;
	}

	@Override
	public String toString() {
		return getId() + " directory:" + isDirectory() + " length:" + getLength() + " md5:" + getMd5();
	}
}
