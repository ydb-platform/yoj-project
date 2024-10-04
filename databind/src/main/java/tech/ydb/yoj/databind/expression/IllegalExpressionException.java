package tech.ydb.yoj.databind.expression;

import lombok.Getter;
import lombok.NonNull;

import java.util.function.UnaryOperator;

public abstract class IllegalExpressionException extends IllegalArgumentException {
    protected IllegalExpressionException(String message) {
        super(message);
    }

    @Getter
    public static abstract class FieldTypeError extends IllegalExpressionException {
        @NonNull
        private final String field;

        protected FieldTypeError(@NonNull String field, UnaryOperator<String> errorMessage) {
            super(errorMessage.apply(field));
            this.field = field;
        }

        static final class FlatFieldExpected extends FieldTypeError {
            FlatFieldExpected(String field) {
                super(field, "Cannot filter by composite field \"%s\""::formatted);
            }
        }

        public static final class UnknownEnumConstant extends FieldTypeError {
            public UnknownEnumConstant(String field, String enumConstant) {
                super(field, f -> "Unknown enum constant for field \"%s\": \"%s\"".formatted(f, enumConstant));
            }
        }

        public static final class StringFieldExpected extends FieldTypeError {
            public StringFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a string value"::formatted);
            }
        }

        public static final class IntegerFieldExpected extends FieldTypeError {
            public IntegerFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with an integer value"::formatted);
            }
        }

        public static final class IntegerBadTimestamp extends FieldTypeError {
            public IntegerBadTimestamp(String field) {
                super(field, "Negative integer value for timestamp field \"%s\", not a valid UNIX epoch timestamp"::formatted);
            }
        }

        public static final class RealFieldExpected extends FieldTypeError {
            public RealFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a floating-point value"::formatted);
            }
        }

        public static final class IntegerToRealInexact extends FieldTypeError {
            public IntegerToRealInexact(String field) {
                super(field, "Integer value magnitude is too large for floating-point field \"%s\""::formatted);
            }
        }

        public static final class BooleanFieldExpected extends FieldTypeError {
            public BooleanFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a boolean value"::formatted);
            }
        }

        public static final class ByteArrayFieldExpected extends FieldTypeError {
            public ByteArrayFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a ByteArray value"::formatted);
            }
        }

        public static final class TimestampFieldExpected extends FieldTypeError {
            public TimestampFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a timestamp value"::formatted);
            }
        }

        public static final class TimestampToIntegerInexact extends FieldTypeError {
            public TimestampToIntegerInexact(String field) {
                super(field, "Timestamp value is too large for integer field \"%s\""::formatted);
            }
        }

        public static final class UuidFieldExpected extends FieldTypeError {
            public UuidFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with an UUID value"::formatted);
            }
        }

        public static final class TupleFieldExpected extends FieldTypeError {
            public TupleFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a tuple value"::formatted);
            }
        }
    }
}
