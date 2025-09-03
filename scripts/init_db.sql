-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Create application user
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'job_scheduler_app') THEN
        CREATE ROLE job_scheduler_app WITH LOGIN PASSWORD 'change_me_in_production';
        RAISE NOTICE 'Created application user job_scheduler_app';
    ELSE
        RAISE NOTICE 'Application user job_scheduler_app already exists';
    END IF;
END
$$;

-- Create ENUM types
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
        CREATE TYPE job_status AS ENUM (
            'pending',
            'running', 
            'completed',
            'failed',
            'cancelled',
            'retrying'
        );
        RAISE NOTICE 'Created job_status enum';
    END IF;
    
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_type') THEN
        CREATE TYPE job_type AS ENUM (
            'email',
            'report',
            'web_scrape', 
            'payment',
            'ml_inference',
            'custom'
        );
        RAISE NOTICE 'Created job_type enum';
    END IF;
END
$$;

-- Create tables
-- Jobs table
CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Job identification and classification
    type job_type NOT NULL,
    status job_status NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 5 CHECK (priority >= 1 AND priority <= 15),
    
    -- Job data
    payload JSONB NOT NULL,
    result JSONB,
    error TEXT,
    
    -- Retry configuration
    max_retries INTEGER NOT NULL DEFAULT 3 CHECK (max_retries >= 0 AND max_retries <= 10),
    retry_count INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    retry_delay BIGINT NOT NULL DEFAULT 300000000000 CHECK (retry_delay >= 0),
    
    -- Scheduling and timing
    scheduled_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    timeout BIGINT NOT NULL DEFAULT 1800000000000 CHECK (timeout > 0),
    
    -- Metadata
    created_by TEXT,
    tags TEXT[] DEFAULT '{}',
    queue_name TEXT NOT NULL DEFAULT 'default',
    worker_id TEXT,
    
    -- Audit timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE,
    
    -- Additional constraints
    CONSTRAINT check_retry_count_valid CHECK (retry_count <= max_retries),
    CONSTRAINT check_completed_jobs_have_timestamp CHECK (
        (status IN ('completed', 'failed', 'cancelled') AND completed_at IS NOT NULL)
        OR (status NOT IN ('completed', 'failed', 'cancelled'))
    ),
    CONSTRAINT check_running_jobs_have_start_time CHECK (
        (status = 'running' AND started_at IS NOT NULL) OR (status != 'running')
    )
);

-- Scheduled jobs table
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    
    -- Job definition
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    cron_expression TEXT NOT NULL,
    
    -- Job template
    job_type job_type NOT NULL,
    payload JSONB NOT NULL,
    priority INTEGER NOT NULL DEFAULT 5 CHECK (priority >= 1 AND priority <= 15),
    max_retries INTEGER NOT NULL DEFAULT 3 CHECK (max_retries >= 0 AND max_retries <= 10),
    timeout BIGINT NOT NULL DEFAULT 1800000000000 CHECK (timeout > 0),
    queue_name TEXT NOT NULL DEFAULT 'default',
    tags TEXT[] DEFAULT '{}',
    
    -- Schedule tracking
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_run TIMESTAMP WITH TIME ZONE,
    next_run TIMESTAMP WITH TIME ZONE,
    
    -- Audit timestamps
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMP WITH TIME ZONE
);

-- Job execution history table
CREATE TABLE IF NOT EXISTS job_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL,
    
    -- Execution details
    attempt_number INTEGER NOT NULL,
    worker_id TEXT,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    
    -- Execution result
    status job_status NOT NULL,
    error TEXT,
    duration_ms INTEGER,
    
    -- Audit
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    
    -- Foreign key
    CONSTRAINT fk_job_executions_job_id FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

-- Create indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_status ON jobs(status) WHERE deleted_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_type ON jobs(type) WHERE deleted_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_priority ON jobs(priority DESC) WHERE deleted_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_queue_name ON jobs(queue_name) WHERE deleted_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_scheduled_at ON jobs(scheduled_at) WHERE deleted_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_created_at ON jobs(created_at DESC) WHERE deleted_at IS NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_updated_at ON jobs(updated_at DESC) WHERE deleted_at IS NULL;

-- Composite indexes for performance
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_worker_pickup 
ON jobs(queue_name, status, priority DESC, created_at) 
WHERE status = 'pending' AND deleted_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_status_priority 
ON jobs(status, priority DESC) WHERE deleted_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_type_status 
ON jobs(type, status) WHERE deleted_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_retry_ready
ON jobs(status, retry_count, max_retries, priority DESC)
WHERE status = 'failed' AND deleted_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_scheduled_ready
ON jobs(scheduled_at, status)
WHERE status = 'pending' AND scheduled_at IS NOT NULL AND deleted_at IS NULL;

-- GIN indexes for advanced queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_payload_gin 
ON jobs USING GIN(payload) WHERE deleted_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_tags_gin 
ON jobs USING GIN(tags) WHERE deleted_at IS NULL;

-- Scheduled jobs indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scheduled_jobs_enabled 
ON scheduled_jobs(enabled) WHERE deleted_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scheduled_jobs_next_run 
ON scheduled_jobs(next_run) WHERE enabled = TRUE AND deleted_at IS NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_scheduled_jobs_name 
ON scheduled_jobs(name) WHERE deleted_at IS NULL;

-- Job executions indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_executions_job_id ON job_executions(job_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_executions_started_at ON job_executions(started_at DESC);

-- Create functions and triggers
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers
DROP TRIGGER IF EXISTS update_jobs_updated_at ON jobs;
CREATE TRIGGER update_jobs_updated_at 
    BEFORE UPDATE ON jobs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_scheduled_jobs_updated_at ON scheduled_jobs;
CREATE TRIGGER update_scheduled_jobs_updated_at 
    BEFORE UPDATE ON scheduled_jobs 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Utility functions
CREATE OR REPLACE FUNCTION get_next_jobs_for_worker(
    p_queue_name TEXT DEFAULT 'default',
    p_limit INTEGER DEFAULT 10
) RETURNS TABLE (
    job_id UUID,
    job_type job_type,
    priority INTEGER,
    payload JSONB,
    max_retries INTEGER,
    retry_count INTEGER,
    timeout BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        j.id, j.type, j.priority, j.payload, j.max_retries, j.retry_count, j.timeout
    FROM jobs j
    WHERE j.queue_name = p_queue_name
      AND j.status = 'pending'
      AND j.deleted_at IS NULL
      AND (j.scheduled_at IS NULL OR j.scheduled_at <= NOW())
    ORDER BY j.priority DESC, j.created_at ASC
    LIMIT p_limit;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION cleanup_old_jobs(
    p_older_than_days INTEGER DEFAULT 30,
    p_batch_size INTEGER DEFAULT 1000
) RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
BEGIN
    DELETE FROM jobs 
    WHERE id IN (
        SELECT id FROM jobs 
        WHERE status IN ('completed', 'cancelled')
          AND completed_at < NOW() - (p_older_than_days || ' days')::INTERVAL
          AND deleted_at IS NULL
        LIMIT p_batch_size
    );
    
    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Create views
CREATE OR REPLACE VIEW job_stats AS
SELECT 
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE status = 'pending') as pending,
    COUNT(*) FILTER (WHERE status = 'running') as running,
    COUNT(*) FILTER (WHERE status = 'completed') as completed,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
    COUNT(*) FILTER (WHERE status = 'cancelled') as cancelled,
    COUNT(*) FILTER (WHERE status = 'retrying') as retrying
FROM jobs 
WHERE deleted_at IS NULL;

CREATE OR REPLACE VIEW job_type_stats AS
SELECT 
    type,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE status = 'pending') as pending,
    COUNT(*) FILTER (WHERE status = 'running') as running,
    COUNT(*) FILTER (WHERE status = 'completed') as completed,
    COUNT(*) FILTER (WHERE status = 'failed') as failed,
    ROUND(AVG(EXTRACT(EPOCH FROM (completed_at - started_at)))::numeric, 2) as avg_duration_seconds
FROM jobs 
WHERE deleted_at IS NULL
GROUP BY type;

CREATE OR REPLACE VIEW queue_stats AS
SELECT 
    queue_name,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE status = 'pending') as pending,
    COUNT(*) FILTER (WHERE status = 'running') as running,
    COUNT(*) FILTER (WHERE status = 'completed') as completed,
    COUNT(*) FILTER (WHERE status = 'failed') as failed
FROM jobs 
WHERE deleted_at IS NULL
GROUP BY queue_name;

-- Grant permissions to application user
GRANT USAGE ON SCHEMA public TO job_scheduler_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON jobs TO job_scheduler_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON scheduled_jobs TO job_scheduler_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON job_executions TO job_scheduler_app;
GRANT SELECT ON job_stats TO job_scheduler_app;
GRANT SELECT ON job_type_stats TO job_scheduler_app;
GRANT SELECT ON queue_stats TO job_scheduler_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO job_scheduler_app;
GRANT EXECUTE ON FUNCTION get_next_jobs_for_worker(TEXT, INTEGER) TO job_scheduler_app;
GRANT EXECUTE ON FUNCTION cleanup_old_jobs(INTEGER, INTEGER) TO job_scheduler_app;
GRANT EXECUTE ON FUNCTION update_updated_at_column() TO job_scheduler_app;

-- Insert sample scheduled jobs
INSERT INTO scheduled_jobs (name, description, cron_expression, job_type, payload, priority) 
VALUES 
    ('daily_cleanup', 'Clean up old completed jobs daily at 2 AM', '0 2 * * *', 'custom', 
     '{"action": "cleanup", "older_than_days": 7}', 5),
    ('weekly_report', 'Generate weekly performance report every Monday at 9 AM', '0 9 * * 1', 'report', 
     '{"type": "performance", "period": "weekly", "recipients": ["admin@company.com"]}', 10),
    ('health_check', 'System health check every 5 minutes', '*/5 * * * *', 'custom', 
     '{"action": "health_check", "services": ["database", "redis", "external_apis"]}', 3)
ON CONFLICT (name) DO NOTHING;

-- Final verification
DO $$
DECLARE
    table_count INTEGER;
    enum_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count 
    FROM information_schema.tables 
    WHERE table_name IN ('jobs', 'scheduled_jobs', 'job_executions')
      AND table_schema = 'public';
      
    SELECT COUNT(*) INTO enum_count
    FROM pg_type 
    WHERE typname IN ('job_status', 'job_type');
      
    IF table_count = 3 AND enum_count = 2 THEN
        RAISE NOTICE 'SUCCESS: All required tables and enums created successfully';
    ELSE
        RAISE NOTICE 'WARNING: Expected 3 tables and 2 enums, found % tables and % enums', table_count, enum_count;
    END IF;
END
$$;

-- Display current status
SELECT 'Database ready! Current job stats:' as status;
SELECT * FROM job_stats;