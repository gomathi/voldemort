package voldemort.hashtrees;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

import voldemort.utils.ByteArray;
import voldemort.utils.ByteUtils;

public class LevelDBExample {

    public static void main(String[] args) throws IOException {

        test();
        Options options = new Options();
        options.createIfMissing();
        DB db = new JniDBFactory().open(new File("/tmp/test/a.txt"), options);
        Random random = new Random();

        byte[] value = "gomathi".getBytes();

        for(int i = 0; i < 1024; i++) {
            db.put(HashTreePersistentStorage.prepareSegmentDataKeyPrefix(random.nextInt(1000),
                                                                         random.nextInt(1000)),
                   value);
        }

        DBIterator itr = db.iterator();
        itr.seekToFirst();
        while(itr.hasNext()) {
            byte[] key = itr.next().getKey();
            HashTreePersistentStorage.readSegmentDataKey(key);
        }
    }

    private static void test() {
        List<ByteArray> byteArray = new ArrayList<ByteArray>();
        for(int i = 0; i < 100; i++) {
            byte[] bytes = new byte[8];
            ByteUtils.writeInt(bytes, new Random().nextInt(1000), 0);
            ByteUtils.writeInt(bytes, new Random().nextInt(1000), 4);
            byteArray.add(new ByteArray(bytes));
        }
        Collections.sort(byteArray);
        for(int i = 0; i < byteArray.size(); i++) {
            System.out.println(ByteUtils.readInt(byteArray.get(i).get(), 0) + ","
                               + ByteUtils.readInt(byteArray.get(i).get(), 4));
        }
    }
}
