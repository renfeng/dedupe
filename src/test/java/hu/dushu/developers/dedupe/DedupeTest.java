package hu.dushu.developers.dedupe;

import org.junit.Test;

import java.io.IOException;

/**
 * Created by frren on 8/18/2015.
 */
public class DedupeTest {

	@Test
	public void test() throws IOException {
		new Dedupe().refresh();
	}
}
