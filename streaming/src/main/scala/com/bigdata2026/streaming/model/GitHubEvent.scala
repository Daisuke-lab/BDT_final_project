package com.bigdata2026.streaming.model

import zio.json._

final case class GitHubEvent(
  @jsonField("event_type")   eventType:   String,
  @jsonField("actor_login")  actorLogin:  String,
  @jsonField("actor_id")     actorId:     Long,
  @jsonField("repo_id")      repoId:      Long,
  @jsonField("repo_name")    repoName:    String,
  @jsonField("created_at")   createdAt:   String,
  @jsonField("ingest_time")  ingestTime:  String,
  @jsonField("trend_weight") trendWeight: Int,
  @jsonField("push_size")    pushSize:    Int,
  @jsonField("push_ref")     pushRef:     String,
  @jsonField("ref_type")     refType:     String,
  @jsonField("fork_repo")    forkRepo:    String,
  @jsonField("release_tag")  releaseTag:  String,
  @jsonField("pr_action")    prAction:    String,
  @jsonField("issue_action") issueAction: String
)

object GitHubEvent {
  implicit val decoder: JsonDecoder[GitHubEvent] = DeriveJsonDecoder.gen[GitHubEvent]
  implicit val encoder: JsonEncoder[GitHubEvent] = DeriveJsonEncoder.gen[GitHubEvent]

  // Envelope: { "id": "...", "source": "github", "payload": "<escaped json>", "timestamp": <Long> }
  // Returns None for malformed envelope or payload schema mismatch — caller skips and commits offset.
  def fromEnvelopeJson(raw: String): Option[GitHubEvent] =
    for {
      envelope <- raw.fromJson[Envelope].toOption
      event    <- envelope.payload.fromJson[GitHubEvent].toOption
    } yield event

  private final case class Envelope(id: String, source: String, payload: String, timestamp: Long)
  private object Envelope {
    implicit val decoder: JsonDecoder[Envelope] = DeriveJsonDecoder.gen[Envelope]
  }
}
