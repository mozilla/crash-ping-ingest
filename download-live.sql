declare latest_release_version NUMERIC default (select major_version from mozdata.telemetry.releases where category = 'major' order by date desc limit 1);
declare min_esr_version NUMERIC default (select major_version from mozdata.telemetry.releases where category = 'esr' order by date desc limit 1);
declare min_version NUMERIC default latest_release_version - 3;

-- Expire old log entries (in case jobs failed removing them).
delete from crash_ping_ingest_external.live_ingest_log
where time < CURRENT_TIMESTAMP() - INTERVAL 1 HOUR;

create temp table selected as (
    -- TODO use crash event ID to deduplicate
    with
    desktop as (
        select
            document_id,
            submission_timestamp,
            metrics.object.crash_stack_traces as stack_traces,
            metrics.object.crash_java_exception as java_exception,
            metrics.object.crash_async_shutdown_timeout as async_shutdown_timeout,
            metrics.string.crash_ipc_channel_error as ipc_channel_error,
            metrics.quantity.memory_oom_allocation_size as oom_size,
            metrics.string.crash_hang as hang,
            metrics.string.memory_js_large_allocation_failure as js_large_allocation_failure,
            coalesce(metrics.string.crash_os, normalized_os) as os,
            coalesce(metrics.string.crash_app_channel, client_info.app_channel) as channel,
            SAFE_CAST(REGEXP_SUBSTR(coalesce(metrics.string.crash_app_display_version, client_info.app_display_version), '[0-9]*') as INT64) as major_version
        from mozdata.firefox_crashreporter.crash_live
    )
    , fenix as (
        select
            document_id,
            submission_timestamp,
            metrics.object.crash_stack_traces as stack_traces,
            metrics.object.crash_java_exception as java_exception,
            metrics.object.crash_async_shutdown_timeout as async_shutdown_timeout,
            metrics.string.crash_ipc_channel_error as ipc_channel_error,
            metrics.quantity.memory_oom_allocation_size as oom_size,
            metrics.string.crash_hang as hang,
            metrics.string.memory_js_large_allocation_failure as js_large_allocation_failure,
            coalesce(metrics.string.crash_os, normalized_os) as os,
            coalesce(metrics.string.crash_app_channel, client_info.app_channel) as channel,
            SAFE_CAST(REGEXP_SUBSTR(coalesce(metrics.string.crash_app_display_version, client_info.app_display_version), '[0-9]*') as INT64) as major_version
        from mozdata.fenix.crash_live
    )
    , focus as (
        select
            document_id,
            submission_timestamp,
            metrics.object.crash_stack_traces as stack_traces,
            metrics.object.crash_java_exception as java_exception,
            metrics.object.crash_async_shutdown_timeout as async_shutdown_timeout,
            metrics.string.crash_ipc_channel_error as ipc_channel_error,
            metrics.quantity.memory_oom_allocation_size as oom_size,
            metrics.string.crash_hang as hang,
            metrics.string.memory_js_large_allocation_failure as js_large_allocation_failure,
            coalesce(metrics.string.crash_os, normalized_os) as os,
            coalesce(metrics.string.crash_app_channel, client_info.app_channel) as channel,
            SAFE_CAST(REGEXP_SUBSTR(coalesce(metrics.string.crash_app_display_version, client_info.app_display_version), '[0-9]*') as INT64) as major_version
        from mozdata.focus_android.crash_live
    )
    , klar as (
        select
            document_id,
            submission_timestamp,
            metrics.object.crash_stack_traces as stack_traces,
            metrics.object.crash_java_exception as java_exception,
            metrics.object.crash_async_shutdown_timeout as async_shutdown_timeout,
            metrics.string.crash_ipc_channel_error as ipc_channel_error,
            metrics.quantity.memory_oom_allocation_size as oom_size,
            metrics.string.crash_hang as hang,
            metrics.string.memory_js_large_allocation_failure as js_large_allocation_failure,
            coalesce(metrics.string.crash_os, normalized_os) as os,
            coalesce(metrics.string.crash_app_channel, client_info.app_channel) as channel,
            SAFE_CAST(REGEXP_SUBSTR(coalesce(metrics.string.crash_app_display_version, client_info.app_display_version), '[0-9]*') as INT64) as major_version
        from mozdata.klar_android.crash_live
    )
    , pings as (
        select distinct
            document_id,
            -- Explicitly format the timestamp for maximum precision because these values will be round-tripped into the output table and joined.
            -- Otherwise the default result string only has millisecond precision and doesn't join correctly.
            FORMAT_TIMESTAMP("%FT%R:%E*S", submission_timestamp) as submission_timestamp,
            NULLIF(TO_JSON_STRING(stack_traces), 'null') as stack_traces,
            NULLIF(TO_JSON_STRING(java_exception), 'null') as java_exception,
            NULLIF(TO_JSON_STRING(async_shutdown_timeout), 'null') as async_shutdown_timeout,
            ipc_channel_error,
            oom_size,
            hang,
            js_large_allocation_failure,
            os,
            channel,
            major_version
        from (
            select * from desktop
            union all
            select * from fenix
            union all
            select * from focus
            union all
            select * from klar
        )
        where 
            submission_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 DAY
            and (
                ((channel = 'release' or channel = 'beta' or channel = 'nightly') and major_version >= min_version)
                or
                (channel = 'esr' and major_version >= min_esr_version)
            )
            and (
                stack_traces is not null
                or java_exception is not null
                or async_shutdown_timeout is not null
                or ipc_channel_error is not null
                or oom_size is not null
                or hang is not null
                or js_large_allocation_failure is not null
            )
    )

    select *
    from pings
    where document_id not in (
        select document_id
        from crash_ping_ingest_external.live_ingest_output
        where submission_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 3 DAY
        union all
        select document_id from crash_ping_ingest_external.live_ingest_log
    )
    limit 1000
);

merge into crash_ping_ingest_external.live_ingest_log T
using selected S
on T.document_id = S.document_id
when not matched then insert (time, task, document_id) values (current_timestamp(), @task_id, S.document_id);

-- Query the count without `live_ingest_log` filtering so we can exit if we are
-- not getting many results to process.
select count(*) as ping_count from selected;

select *
from selected
where document_id in (
    select document_id from crash_ping_ingest_external.live_ingest_log where task = @task_id
);

drop table selected;
