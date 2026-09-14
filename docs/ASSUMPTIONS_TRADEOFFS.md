# Assumptions and Tradeoffs

## 1. API authentication

The implementation follows the authentication behavior verified during API exploration and keeps the token outside source control. The production token is read from AWS Secrets Manager.

## 2. Two configured media IDs

The project scope contains only two media IDs. Media metadata is therefore re-read each day instead of maintaining a more complex change-detection service. This is cheap, simple, and robust for the current scope.

## 3. Actual API grain takes precedence over the simplified conceptual model

The sampled Wistia responses expose daily media metrics, aggregate media metrics, and visitor-level events at different grains. The Gold design separates these grains instead of repeating media-level measures across visitor rows, which would introduce double counting.

## 4. Daily play rate

The Gold media/day fact derives `play_rate` as `play_count / load_count` so the metric is consistent with the daily grain. The aggregate API play-rate field is retained in the Silver summary dataset rather than copied to every daily record.

## 5. Channel attribution

The sampled Wistia media schema did not expose a reliable Facebook/YouTube `channel` attribute. The pipeline does not fabricate one. If the business supplies a mapping, it can be added as a small reference/configuration dimension.

## 6. URL field

A top-level media URL was not observed in the sampled media metadata response. Event-level `media_url`/`embed_url` values are preserved where available rather than inventing a media URL.

## 7. Incremental window and overlap

The events pipeline uses a stored watermark plus an overlap window. Re-reading a small overlap intentionally creates possible duplicates in Bronze; Silver deduplication makes the pipeline idempotent while protecting against delayed events.

## 8. Watermark storage in S3

S3 is used for `control/watermarks.json` instead of DynamoDB because the state is tiny and updated once per successful daily pipeline. DynamoDB would add operational complexity without material benefit at this scale.

## 9. Watermark commit after Gold

Bronze does not advance the production watermark. A separate commit job runs only after Gold succeeds. This favors correctness over speed and prevents a partially failed pipeline from skipping a date range on the next run.

## 10. Full overwrite in curated layers

Silver and Gold currently use overwrite semantics. This is acceptable for the small project volume and makes reruns deterministic. At larger scale, partition-level overwrite or merge/upsert behavior would reduce rewrite cost.

## 11. Parquet and partitioning

Silver/Gold use Snappy Parquet for columnar scans and compression. Large facts are partitioned by date-oriented fields to support Athena pruning. Very small dimensions are not partitioned.

## 12. Athena instead of a dedicated warehouse

The structured Gold model is stored in S3, cataloged in Glue, and queried with Athena. A Redshift cluster is not justified by the current volume, concurrency, or SLA. Redshift remains a future option if interactive BI concurrency or warehouse-specific requirements emerge.

## 13. Dashboard

Dashboarding is treated as optional. The project prioritizes the required automated pipeline, structured model, SQL validation, production operation, CI/CD, and documentation.

## 14. PII

Visitor/event responses can contain IP, email, names, and location. The project bucket remains private and access should be least-privilege. A production enterprise design would additionally consider masking/tokenization, retention policies, and separate restricted views for PII.

## 15. GitHub OIDC subject

The repository uses GitHub OIDC for AWS deployment. The AWS role trust policy is restricted to the actual OIDC `sub` claim of this repository and `main` branch. The current GitHub subject format includes immutable owner/repository IDs, so future repository migration or recreation may require updating the IAM trust condition.
