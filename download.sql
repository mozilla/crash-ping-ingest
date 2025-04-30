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
            (ROW_NUMBER() over ()) as id,
            
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
            end) as target_sample_count,
            
            *
        from UNNEST(oses) as os
        cross join UNNEST(channels) as channel
        cross join (
            select * from UNNEST(process_types) as process_type cross join (select STRING(null) as utility_actor)
            union all
            select * from (select 'utility' as process_type) cross join UNNEST(utility_actors) as utility_actor
        )
    );

-- Because we want to output the config table later with the counts of pings
-- which matched each configuration, we materialize temporary tables here
-- rather than use CTEs (since CTEs would be recomputed and we need these
-- tables for both the config query and final output query).
create temp table pings as (
    -- Desktop and Android have slightly different tables because their metrics
    -- differ, so we can't simply union them without first extracting our
    -- fields of interest.
    with
        desktop as (
            select
                document_id,
                submission_timestamp,
                metrics.object.crash_stack_traces as stack_traces,
                metrics.string.crash_moz_crash_reason as moz_crash_reason,
                metrics.string.crash_ipc_channel_error as ipc_channel_error,
                metrics.quantity.memory_oom_allocation_size as oom_size,
                normalized_os as os,
                metrics.string.crash_app_channel as channel,
                metrics.string.crash_app_display_version as display_version,
                metrics.string.crash_process_type as process_type,
                metrics.string_list.crash_utility_actors_name as utility_actors_name
            from firefox_desktop.desktop_crashes
        ),
        android as (
            select
                document_id,
                submission_timestamp,
                metrics.object.crash_stack_traces as stack_traces,
                metrics.object.crash_java_exception as java_exception,
                metrics.string.crash_moz_crash_reason as moz_crash_reason,
                metrics.string.crash_ipc_channel_error as ipc_channel_error,
                metrics.quantity.memory_oom_allocation_size as oom_size,
                normalized_os as os,
                metrics.string.crash_app_channel as channel,
                metrics.string.crash_app_display_version as display_version,
                metrics.string.crash_process_type as process_type
            from fenix.crash
        )
    select
        config.id as config_id,
        target_sample_count,
        document_id,
        -- Explicitly format the timestamp for maximum precision because these values will be round-tripped into the ingest output table and joined.
        -- Otherwise the default result string only has millisecond precision and doesn't join correctly.
        FORMAT_TIMESTAMP("%FT%R:%E*S", submission_timestamp) as submission_timestamp,
        IF(stack_traces is null, null, TO_JSON_STRING(stack_traces)) as stack_traces,
        IF(java_exception is null, null, TO_JSON_STRING(java_exception)) as java_exception,
        moz_crash_reason,
        ipc_channel_error,
        oom_size,
        data.os,
        data.channel,
    from (select * from desktop outer union all by name select * from android) as data
    join config
        on config.os = data.os
        and config.channel = data.channel
        and (SAFE_CAST(REGEXP_SUBSTR(display_version, '[0-9]*') as INT64)) = version
        and config.process_type = data.process_type
        and (
                config.process_type != 'utility'
                or (
                    (utility_actor is null and ARRAY_LENGTH(IFNULL(utility_actors_name, [])) = 0)
                    or (utility_actor in UNNEST(utility_actors_name))
                )
        )
    where 
        DATE(submission_timestamp) = @date
        and (stack_traces is not null or java_exception is not null)
);

create temp table config_counts as (
    select config_id, COUNT(*) as total_config_count from pings group by config_id
);

-- Output the configuration with the total counts to record how the data was selected.
select STRING(@date) as date, total_config_count as count, config.*
from config_counts
join config on config_id = config.id;
 
-- Just in case, select distinct (it's possible, though unlikely, that we might
-- have duplicates from the way utility actors are selected: one ping may
-- have more than one actor set).
select distinct
    document_id,
    config_id,
    submission_timestamp,
    stack_traces,
    java_exception,
    moz_crash_reason,
    ipc_channel_error,
    oom_size,
    os,
    channel
from pings
join config_counts using (config_id)
-- To ensure a random sample, we select based on the proportion of the total.
where RAND() <= (target_sample_count / total_config_count);

-- Clean up by dropping temp tables.
drop table config;
drop table pings;
drop table config_counts;
