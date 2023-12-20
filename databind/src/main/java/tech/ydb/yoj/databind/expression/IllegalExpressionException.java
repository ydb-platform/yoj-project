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

        static final class UnknownEnumConstant extends FieldTypeError {
            UnknownEnumConstant(String field, String enumConstant) {
                super(field, f -> "Unknown enum constant for field \"%s\": \"%s\"".formatted(f, enumConstant));
            }
        }

        static final class StringFieldExpected extends FieldTypeError {
            StringFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a string value"::formatted);
            }
        }

        static final class IntegerFieldExpected extends FieldTypeError {
            IntegerFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with an integer value"::formatted);
            }
        }

        static final class RealFieldExpected extends FieldTypeError {
            RealFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a floating-point value"::formatted);
            }
        }

        static final class BooleanFieldExpected extends FieldTypeError {
            BooleanFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a boolean value"::formatted);
            }
        }

        static final class DateTimeFieldExpected extends FieldTypeError {
            DateTimeFieldExpected(String field) {
                super(field, "Type mismatch: cannot compare field \"%s\" with a date-time value"::formatted);
            }
        }
    }
}
