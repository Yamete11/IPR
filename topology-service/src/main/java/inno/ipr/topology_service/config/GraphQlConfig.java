package inno.ipr.topology_service.config;

import graphql.GraphQLContext;
import graphql.execution.CoercedVariables;
import graphql.language.ArrayValue;
import graphql.language.BooleanValue;
import graphql.language.EnumValue;
import graphql.language.FloatValue;
import graphql.language.IntValue;
import graphql.language.NullValue;
import graphql.language.ObjectValue;
import graphql.language.StringValue;
import graphql.language.Value;
import graphql.schema.Coercing;
import graphql.schema.CoercingParseLiteralException;
import graphql.schema.GraphQLScalarType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.execution.RuntimeWiringConfigurer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Binds the {@code scalar JSON} declared in {@code schema.graphqls} so vertex/edge {@code properties}
 * serialise as real JSON objects instead of strings.
 *
 * <p>The coercing is defined inline against the graphql-java version Spring Boot manages, rather
 * than pulling {@code graphql-java-extended-scalars} — that library has no release aligned with the
 * graphql-java 25.0 brought in by Spring Boot 4.1.0, so depending on it forces a cross-major
 * override ({@code 24.1 -> 25.0}). {@code properties} values arrive from {@code AgtypeParser} already
 * as plain {@code Map}/scalar structures, so {@link #serialize} is a pass-through; {@link #parseLiteral}
 * is implemented for completeness even though the read-only schema never takes JSON as input.
 */
@Configuration
public class GraphQlConfig {

    @Bean
    public RuntimeWiringConfigurer jsonScalarConfigurer() {
        GraphQLScalarType json = GraphQLScalarType.newScalar()
                .name("JSON")
                .description("A JSON value (object, array, or scalar).")
                .coercing(new JsonCoercing())
                .build();
        return wiring -> wiring.scalar(json);
    }

    /** Pass-through coercing for arbitrary JSON-compatible Java values. */
    private static final class JsonCoercing implements Coercing<Object, Object> {

        @Override
        public Object serialize(Object dataFetcherResult, GraphQLContext context, Locale locale) {
            return dataFetcherResult;
        }

        @Override
        public Object parseValue(Object input, GraphQLContext context, Locale locale) {
            return input;
        }

        @Override
        public Object parseLiteral(Value<?> input, CoercedVariables variables,
                                   GraphQLContext context, Locale locale) {
            return fromAst(input);
        }

        private static Object fromAst(Value<?> value) {
            return switch (value) {
                case NullValue ignored -> null;
                case StringValue s -> s.getValue();
                case BooleanValue b -> b.isValue();
                case EnumValue e -> e.getName();
                case IntValue i -> i.getValue();
                case FloatValue f -> f.getValue();
                case ArrayValue array -> {
                    List<Object> list = new ArrayList<>();
                    for (Value<?> item : array.getValues()) {
                        list.add(fromAst(item));
                    }
                    yield list;
                }
                case ObjectValue object -> {
                    Map<String, Object> map = new LinkedHashMap<>();
                    object.getObjectFields().forEach(field -> map.put(field.getName(), fromAst(field.getValue())));
                    yield map;
                }
                default -> throw new CoercingParseLiteralException(
                        "Unsupported JSON literal: " + value.getClass().getSimpleName());
            };
        }
    }
}
