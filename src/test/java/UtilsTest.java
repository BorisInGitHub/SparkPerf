import common.Utils;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by breynard on 24/10/16.
 */
public class UtilsTest {
    @Test
    public void test() throws IOException {
        String fileName = Utils.createFile();
        System.out.println("File : "+fileName);
    }
}
