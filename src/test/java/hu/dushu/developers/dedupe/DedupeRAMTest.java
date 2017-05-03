package hu.dushu.developers.dedupe;

import okhttp3.OkHttpClient;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by renfeng on 5/3/17.
 */
public class DedupeRAMTest {
	@Test
	public void refresh() throws IOException {
		Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.FINE);
		String home = System.getProperty("user.home");
		new DedupeRAM().refresh(new File[]{
				new File(home, "Desktop"),
				new File(home, "Documents"),
				new File(home, "Downloads"),
				new File(home, "Music"),
				new File(home, "Pictures"),
				new File(home, "Public"),
				new File(home, "Videos"),
		});
	}
}