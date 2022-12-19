package org.hypertrace.gateway.service.entity.converter;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator;
import org.hypertrace.entity.query.service.v1.BulkUpdateAllMatchingFilterRequest;
import org.hypertrace.entity.query.service.v1.BulkUpdateAllMatchingFilterResponse;
import org.hypertrace.gateway.service.common.converters.Converter;
import org.hypertrace.gateway.service.common.converters.EntityServiceAndGatewayServiceConverter;
import org.hypertrace.gateway.service.v1.common.ColumnIdentifier;
import org.hypertrace.gateway.service.v1.common.Filter;
import org.hypertrace.gateway.service.v1.common.LiteralConstant;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesRequest;
import org.hypertrace.gateway.service.v1.entity.BulkUpdateAllMatchingEntitiesResponse;
import org.hypertrace.gateway.service.v1.entity.Update;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation;
import org.hypertrace.gateway.service.v1.entity.UpdateOperation.Operator;

public class EntityConverterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<
            Converter<
                BulkUpdateAllMatchingEntitiesRequest, BulkUpdateAllMatchingFilterRequest>>() {})
        .to(BulkUpdateAllMatchingEntitiesRequestConverter.class);
    bind(new TypeLiteral<
            Converter<
                BulkUpdateAllMatchingFilterResponse, BulkUpdateAllMatchingEntitiesResponse>>() {})
        .to(BulkUpdateAllMatchingFilterResponseConverter.class);
    bind(new TypeLiteral<Converter<Update, org.hypertrace.entity.query.service.v1.Update>>() {})
        .to(UpdateConverter.class);
    bind(new TypeLiteral<Converter<UpdateOperation, AttributeUpdateOperation>>() {})
        .to(UpdateOperationConverter.class);
    bind(new TypeLiteral<Converter<Operator, AttributeUpdateOperator>>() {})
        .to(UpdateOperatorConverter.class);

    bind(new TypeLiteral<Converter<Filter, org.hypertrace.entity.query.service.v1.Filter>>() {})
        .toInstance(EntityServiceAndGatewayServiceConverter::convertToEntityServiceFilter);
    bind(new TypeLiteral<
            Converter<
                ColumnIdentifier, org.hypertrace.entity.query.service.v1.ColumnIdentifier>>() {})
        .toInstance(EntityServiceAndGatewayServiceConverter::convertToQueryColumnIdentifier);
    bind(new TypeLiteral<
            Converter<
                LiteralConstant, org.hypertrace.entity.query.service.v1.LiteralConstant>>() {})
        .toInstance(EntityServiceAndGatewayServiceConverter::convertToQueryLiteral);
  }
}
