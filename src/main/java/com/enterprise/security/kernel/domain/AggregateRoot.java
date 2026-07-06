package com.enterprise.security.kernel.domain;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base class for all DDD Aggregate Roots.
 *
 * Responsibilities:
 *  - owns the aggregate identity ({@link #id})
 *  - buffers domain events raised during a command so the application layer can
 *    dispatch them AFTER the aggregate is persisted (transactional outbox style)
 *  - tracks an optimistic-concurrency version
 *
 * Two construction modes are supported:
 *  1. {@code super(id)} — aggregates that own identity through this base class
 *     (e.g. auth {@code User}, {@code UserSession}).
 *  2. {@code super()} (no-arg) — aggregates that keep their own {@code id} field
 *     and are reconstituted via Lombok {@code @Builder}; they expose their own
 *     id getter while the inherited {@link #id} stays null.
 */
public abstract class AggregateRoot<ID> {

    @Getter
    private final ID id;

    private long version = 0;

    // K1-10: synchronized so an aggregate accidentally shared across threads cannot
    // corrupt the buffer. The compound copy+clear / copy operations below also lock
    // on this list. (Single-thread-per-aggregate use sees only uncontended locks.)
    private final List<DomainEvent> uncommittedEvents =
            Collections.synchronizedList(new ArrayList<>());

    protected AggregateRoot(ID id) {
        this.id = id;
    }

    /**
     * No-arg constructor for @Builder-reconstituted aggregates that manage their
     * own identity field. The inherited {@link #id} remains null and is unused;
     * such aggregates publish events using their own id via {@code aggregateId()}.
     */
    protected AggregateRoot() {
        this.id = null;
    }

    /** Register a domain event to be dispatched after the aggregate is persisted. */
    protected void registerEvent(DomainEvent event) {
        if (event != null) {
            uncommittedEvents.add(event);
        }
    }

    /** Retrieve and clear all uncommitted events (called by the app layer after save). */
    public List<DomainEvent> pullUncommittedEvents() {
        synchronized (uncommittedEvents) {
            List<DomainEvent> events = new ArrayList<>(uncommittedEvents);
            uncommittedEvents.clear();
            return Collections.unmodifiableList(events);
        }
    }

    /** True if the aggregate raised events that have not yet been pulled. */
    public boolean hasUncommittedEvents() {
        return !uncommittedEvents.isEmpty();
    }

    /**
     * Optimistic-concurrency version of this aggregate.
     *
     * <p><b>K1-06:</b> abstract on purpose — every aggregate must declare where its
     * version comes from, so generic persistence infrastructure can never silently
     * read a meaningless {@code 0}:</p>
     * <ul>
     *   <li>{@code super(id)} aggregates (and those with no version of their own)
     *       return {@link #baseVersion()};</li>
     *   <li>aggregates that keep their own {@code version} field (e.g. configuration
     *       {@code ConfigEntry}) return that field.</li>
     * </ul>
     * Named {@code aggregateVersion()} (not {@code getVersion()}) so it never
     * collides with a domain aggregate's own {@code version} getter.
     */
    public abstract long aggregateVersion();

    /** Version tracked by this base class — for {@code super(id)} aggregates to expose. */
    protected final long baseVersion() {
        return version;
    }

    protected void incrementVersion() {
        this.version++;
    }

    // ── Event Sourcing support (additive; non-event-sourced aggregates ignore these) ──

    /**
     * Non-clearing view of the buffered events — used to append to an event store
     * within the same transaction as the state write, BEFORE the application layer
     * pulls-and-clears them for Kafka dispatch.
     */
    public List<DomainEvent> peekUncommittedEvents() {
        synchronized (uncommittedEvents) {
            return Collections.unmodifiableList(new ArrayList<>(uncommittedEvents));
        }
    }

    /**
     * Fold a single historical event into this aggregate's state. Default is a no-op
     * so ordinary (state-table) aggregates are unaffected; event-sourced aggregates
     * override this to reconstruct state from their own event stream.
     */
    protected void apply(DomainEvent event) {
        // no-op by default; event-sourced aggregates override
    }

    /**
     * Replay a historical event during aggregate reconstitution.
     */
    public final void replayEvent(DomainEvent event) {
        apply(event);
        incrementVersion();
    }


    /**
     * Rebuild aggregate state by replaying a historical event stream (oldest first).
     * Replayed events are NOT re-registered as uncommitted (they are already persisted).
     */
    public void loadFromHistory(List<DomainEvent> history) {
        if (history == null) return;
        for (DomainEvent event : history) {
            apply(event);
            incrementVersion();
        }
    }

    /**
     * Single-path mutation helper for event-sourced aggregates: apply the change to
     * state, buffer the event for post-persist dispatch, and bump the version. Ordinary
     * aggregates may keep their existing "mutate + registerEvent" style instead.
     */
    protected void applyChange(DomainEvent event) {
        apply(event);
        registerEvent(event);
        incrementVersion();
    }
}
