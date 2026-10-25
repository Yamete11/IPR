package inno.ipr.topology_service.config;

import graphql.GraphQLContext;
import graphql.schema.Coercing;
import graphql.schema.GraphQLScalarType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.graphql.execution.RuntimeWiringConfigurer;

import java.util.Locale;

/**
 * Binds the {@code scalar JSON} declared in {@code schema.graphqls} so vertex/edge {@code properties}
 * serialise as real JSON objects instead of strings.
 *
 * <p>The coercing is defined inline rather than pulling {@code graphql-java-extended-scalars}, which
 * has no release aligned with the graphql-java 25.0 that Spring Boot 4.1.0 manages.
 *
 * <p>{@code JsonCoercing} only implements {@code serialize}, as a pass-through: {@code AgtypeParser}
 * already yields plain {@code Map}/scalar values. The schema is read-only and never takes
 * {@code JSON} as input, so the input-side methods keep {@link Coercing}'s defaults.
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

    private static final class JsonCoercing implements Coercing<Object, Object> {

        @Override
        public Object serialize(Object dataFetcherResult, GraphQLContext context, Locale locale) {
            return dataFetcherResult;
        }
    }
}
