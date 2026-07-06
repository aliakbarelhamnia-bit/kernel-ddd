package com.enterprise.security.kernel.domain;

import java.util.Optional;

/**
 * Generic, multi-tenant repository port (output port in Hexagonal Architecture).
 *
 * Tenant scoping is explicit on every method because aggregates are strictly
 * isolated per tenant (PostgreSQL RLS + {@code tenant_id} predicate). Each
 * bounded context provides a concrete JDBC adapter and is free to add
 * domain-specific finders on top of this base contract.
 *
 * @param <T>  the aggregate root type
 * @param <ID> the aggregate identity type
 */
public interface DomainRepository<T extends AggregateRoot<ID>, ID> {

    /** Insert or update the aggregate; returns the persisted instance. */
    T save(T aggregate);

    /** Load an aggregate by identity within a tenant. */
    Optional<T> findById(ID id, String tenantId);

    /** True if an aggregate with this identity exists within the tenant. */
    boolean existsById(ID id, String tenantId);

    /** Remove (or soft-remove) the aggregate by identity within a tenant. */
    void delete(ID id, String tenantId);
}
