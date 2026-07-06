package com.enterprise.security.kernel.domain;

import java.time.Instant;
import java.util.UUID;

/**
 * Marker interface for all Domain Events.
 * Domain events are immutable facts about something that happened in the domain.
 */
public interface DomainEvent {

    /**
     * Unique identifier for this event instance.
     */
    UUID eventId();

    /**
     * Timestamp when this event occurred.
     */
    Instant occurredAt();

    /**
     * The aggregate type that produced this event.
     */
    String aggregateType();

    /**
     * The aggregate ID that produced this event.
     */
    String aggregateId();

    /**
     * Event schema version for backward compatibility.
     */
    default int schemaVersion() {
        return 1;
    }

    /**
     * Tenant this event belongs to (multi-tenancy).
     */
    String tenantId();

    /**
     * Correlation ID for distributed tracing.
     */
    String correlationId();
}
