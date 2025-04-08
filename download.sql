declare oses ARRAY<STRING>;
declare channels ARRAY<STRING>;
declare process_types ARRAY<STRING>;
declare utility_actors ARRAY<STRING>;

set oses = ['Android', 'Linux', 'Mac', 'Windows'];
set channels = ['nightly', 'beta', 'release'];
set process_types = ['main', 'content', 'gpu', 'rdd', 'socket', 'gmplugin', 'utility'];
set utility_actors = ['audio-decoder-generic', 'audio-decoder-applemedia', 'audio-decoder-wmf', 'mf-media-engine', 'js-oracle', 'windows-utils', 'windows-file-dialog'];

-- We materialize a temp table of configurations because the generated row numbers must be consistent.
-- This table contains all combinations of the oses, channels, process_types, and utility process + utility_actors.
create temp table config as (
        select
            (ROW_NUMBER() over ()) as groupid,
            
            -- Choose version based on channel
            (case channel
                when 'release' then @release_version
                when 'beta' then @release_version + 1
                when 'nightly' then @release_version + 2
                else ERROR("unknown channel")
            end) as version,
            
            -- Choose sample count based on channel
            (case channel
                when 'release' then 5000
                -- Set very high counts for nightly and beta, to essentially always process all pings for these channels.
                else 50000
            end) as sample_count,
            
            *
        from UNNEST(oses) as os
        cross join UNNEST(channels) as channel
        cross join (
            select * from UNNEST(process_types) as process_type cross join (select 'NONE' as utility_actor)
            union all
            select * from (select 'utility' as process_type) cross join UNNEST(utility_actors) as utility_actor
        )
    );

with
    desktop as (
        select
            groupid,
            sample_count,
            document_id,
            -- Explicitly format the timestamp for maximum precision because these values will be round-tripped into the ingest output table and joined.
            -- Otherwise the default result string only has millisecond precision and doesn't join correctly.
            FORMAT_TIMESTAMP("%FT%R:%E*S", submission_timestamp) as submission_timestamp,
            TO_JSON_STRING(metrics.object.crash_stack_traces) as stack_traces,
            STRING(null) as java_exception,
            metrics.string.crash_moz_crash_reason as moz_crash_reason,
            metrics.string.crash_ipc_channel_error as ipc_channel_error,
            metrics.quantity.memory_oom_allocation_size as oom_size,
            os,
            channel
        from firefox_desktop.desktop_crashes
        join config
            on normalized_os = os
            and client_info.app_channel = channel
            and metrics.string.crash_process_type = process_type
            and (process_type != 'utility' or
                 ((utility_actor = 'NONE' and ARRAY_LENGTH(metrics.string_list.crash_utility_actors_name) = 0) or
                  (utility_actor IN UNNEST(metrics.string_list.crash_utility_actors_name))))
        where
            DATE(submission_timestamp) = @date
            and (SAFE_CAST(REGEXP_SUBSTR(client_info.app_display_version, '[0-9]*') as INT64)) = version 
            and metrics.object.crash_stack_traces is not null
    ),
    -- The android table is almost the same as desktop, but different enough that we can't union with it (the glean struct fields being different causes problems).
    android as (
        select
            groupid,
            sample_count,
            document_id,
            -- Explicitly format the timestamp for maximum precision because these values will be round-tripped into the ingest output table and joined.
            -- Otherwise the default result string only has millisecond precision and doesn't join correctly.
            FORMAT_TIMESTAMP("%FT%R:%E*S", submission_timestamp) as submission_timestamp,
            TO_JSON_STRING(metrics.object.crash_stack_traces) as stack_traces,
            TO_JSON_STRING(metrics.object.crash_java_exception) as java_exception,
            metrics.string.crash_moz_crash_reason as moz_crash_reason,
            metrics.string.crash_ipc_channel_error as ipc_channel_error,
            metrics.quantity.memory_oom_allocation_size as oom_size,
            os,
            channel
        from fenix.crash
        join config
            on normalized_os = os
            and client_info.app_channel = channel
            and metrics.string.crash_process_type = process_type
            and (process_type != 'utility' or utility_actor = 'NONE')
        where
            DATE(submission_timestamp) = @date
            and (SAFE_CAST(REGEXP_SUBSTR(client_info.app_display_version, '[0-9]*') as INT64)) = version
            and (metrics.object.crash_stack_traces is not null or metrics.object.crash_java_exception is not null)
        ),
    combined as (select * from desktop union all select * from android),
    group_counts as (select groupid, COUNT(*) as total_group_count from combined group by groupid)
 
-- Just in case, select distinct (we might have duplicates from the way utility actors are checked).
select distinct
    document_id,
    submission_timestamp,
    stack_traces,
    java_exception,
    moz_crash_reason,
    ipc_channel_error,
    oom_size,
    os,
    channel
from combined
join group_counts using (groupid)
-- To ensure a random sample, we select based on the proportion of the total.
where RAND() <= (sample_count / total_group_count)
