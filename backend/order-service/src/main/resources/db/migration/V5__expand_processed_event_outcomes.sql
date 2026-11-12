ALTER TABLE processed_events DROP CONSTRAINT chk_processed_outcome;

ALTER TABLE processed_events
    ADD CONSTRAINT chk_processed_outcome CHECK (outcome IN (
        'SUCCESS',
        'ALREADY_APPLIED',
        'SKIPPED_DUPLICATE',
        'SKIPPED_IGNORED_EVENT_TYPE',
        'ORDER_NOT_FOUND',
        'TERMINAL_STATE_IGNORED',
        'ILLEGAL_TRANSITION_IGNORED'
    ));
