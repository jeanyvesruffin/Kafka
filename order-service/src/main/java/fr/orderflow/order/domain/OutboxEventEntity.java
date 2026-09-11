package fr.orderflow.order.domain;

import jakarta.persistence.*;

import java.time.Instant;

/**
 * Ligne d'outbox : un evenement en attente de publication.
 *
 * <p>Coeur du pattern Transactional Outbox. L'insertion de cette ligne se fait
 * dans LA MEME transaction que la modification metier. Tant que la transaction
 * n'est pas commitee, l'evenement n'existe pas ; des qu'elle l'est, l'evenement
 * est garanti de partir, meme si le service crashe juste apres.
 */
@Entity
@Table(name = "outbox_event",
        indexes = @Index(name = "idx_outbox_unpublished", columnList = "published, created_at"))
public class OutboxEventEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    /**
     * Agregat concerne — l'orderId. Sert aussi de cle de partition.
     */
    @Column(name = "aggregate_id", nullable = false, length = 64)
    private String aggregateId;

    @Column(name = "event_id", nullable = false, unique = true, length = 64)
    private String eventId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "topic", nullable = false, length = 128)
    private String topic;

    @Lob
    @Column(name = "payload", nullable = false)
    private String payload;

    @Column(name = "correlation_id", nullable = false, length = 64)
    private String correlationId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "published", nullable = false)
    private boolean published;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEventEntity() {
        // requis par JPA
    }

    public OutboxEventEntity(String id, String aggregateId, String eventId, String eventType,
                             String topic, String payload, String correlationId, Instant createdAt) {
        this.id = id;
        this.aggregateId = aggregateId;
        this.eventId = eventId;
        this.eventType = eventType;
        this.topic = topic;
        this.payload = payload;
        this.correlationId = correlationId;
        this.createdAt = createdAt;
        this.published = false;
    }

    public void markPublished(Instant now) {
        this.published = true;
        this.publishedAt = now;
    }

    public String getId() {
        return id;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getEventId() {
        return eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public String getTopic() {
        return topic;
    }

    public String getPayload() {
        return payload;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public boolean isPublished() {
        return published;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
