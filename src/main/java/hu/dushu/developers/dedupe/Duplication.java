package hu.dushu.developers.dedupe;

/**
 * Created by renfeng on 5/7/17.
 */
public class Duplication implements Comparable<Duplication> {

	private long length;
	private long copies;

	public long getLength() {
		return length;
	}

	public void setLength(long length) {
		this.length = length;
	}

	public long getCopies() {
		return copies;
	}

	public void setCopies(long copies) {
		this.copies = copies;
	}

	@Override
	public int compareTo(Duplication o) {
		return -Long.compare(getLength() * getCopies(), o.getLength() * o.getCopies());
	}
}
