package tech.ydb.yoj.repository.test.sample.model.annotations;

import java.util.Objects;


public class Digest implements YojString {
    private final String algorithm;
    private final String digest;

    protected Digest(String algorithm, String digest) {
        this.algorithm = algorithm;
        this.digest = digest;
    }

    @Override
    public String toString() {
        return algorithm + ":" + digest;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        Digest digest1 = (Digest) object;
        return Objects.equals(algorithm, digest1.algorithm) && Objects.equals(digest, digest1.digest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(algorithm, digest);
    }
}
