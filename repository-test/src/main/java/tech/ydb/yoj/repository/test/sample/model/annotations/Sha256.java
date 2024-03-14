package tech.ydb.yoj.repository.test.sample.model.annotations;

public class Sha256 extends Digest {
    private static final String SHA_256 = "SHA256";

    public Sha256(String digest) {
        super(SHA_256, digest);
    }

    public static Sha256 valueOf(String value) {
        String[] parsed = value.split(":");
        if (parsed.length != 2 || !SHA_256.equals(parsed[0])) {
            throw new IllegalArgumentException();
        }
        return new Sha256(parsed[1]);
    }
}
