package tech.ydb.yoj.util.lang;

import java.lang.StackWalker.StackFrame;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.lang.StackWalker.Option.RETAIN_CLASS_REFERENCE;

public final class CallStack {
    private final ConcurrentMap<FrameKey, Object> mapperCache = new ConcurrentHashMap<>();

    public FrameResult findCallingFrame() {
        return new FrameResult();
    }

    public class FrameResult {
        private StackFrame frameCached;
        private final List<Predicate<StackFrame>> predicates = new ArrayList<>();

        private FrameResult() {
        }

        public FrameResult skipPackage(Package packageToSkip) {
            return skipFramesWhile(frame -> frame.getDeclaringClass().getPackage().equals(packageToSkip));
        }

        public FrameResult skipPackage(String packageToSkip) {
            return skipFramesWhile(frame -> frame.getDeclaringClass().getPackage().getName().equals(packageToSkip));
        }

        public FrameResult skipPackages(Set<String> packagesToSkip) {
            return skipFramesWhile(frame -> packagesToSkip.contains(frame.getDeclaringClass().getPackage().getName()));
        }

        public FrameResult skipFramesWhile(Predicate<StackFrame> predicate) {
            predicates.add(predicate);
            return this;
        }

        public StackFrame frame() {
            return findFrame();
        }

        public <T> T map(Function<StackFrame, T> mapper) {
            return map(mapper, null);
        }

        @SuppressWarnings("unchecked")
        public <T> T map(Function<StackFrame, T> mapper, Object extraKey) {
            StackFrame frame = findFrame();
            return (T) mapperCache.computeIfAbsent(new FrameKey(frame, extraKey), __ -> mapper.apply(frame));
        }

        private StackFrame findFrame() {
            if (frameCached != null) {
                return frameCached;
            }
            frameCached = StackWalker.getInstance(RETAIN_CLASS_REFERENCE).walk(stream -> {
                stream = stream.dropWhile(f -> f.getDeclaringClass().equals(getClass()));
                for (var predicate : predicates) {
                    stream = stream.dropWhile(predicate);
                }
                return stream.findFirst()
                        .orElseThrow(() -> new IllegalStateException("Stacktrace doesn't contain matching frames"));
            });
            return frameCached;
        }
    }

    private record FrameKey(String className, String methodName, int lineNumber, Object extraKey) {
        FrameKey(StackFrame frame, Object extraKey) {
            this(frame.getClassName(), frame.getMethodName(), frame.getLineNumber(), extraKey);
        }
    }
}
