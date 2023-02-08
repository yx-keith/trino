package io.trino.server.ui.query.editor.protocol;

import com.google.common.base.Predicate;

/**
 * @author yaoxiao
 * @version 1.0
 * @date 2023/2/8 15:46
 */
public enum JobState
{
    /**
     * Query has been accepted and is awaiting execution.
     */
    QUEUED(false),
    /**
     * Query is waiting for the required resources (beta).
     */
    WAITING_FOR_RESOURCES(false),
    /**
     * Query is being dispatched to a coordinator.
     */
    DISPATCHING(false),
    /**
     * Query is being planned.
     */
    PLANNING(false),
    /**
     * Query execution is being started.
     */
    STARTING(false),
    /**
     * Query has at least one task in the output stage.
     */
    RUNNING(false),
    /**
     * Failed tasks will be re-scheduled. Waiting for old stages/tasks to finish.
     */
    RESCHEDULING(false),
    /**
     * Resume execution of rescheduled tasks, after old stages/tasks finish.
     */
    RESUMING(false),
    /**
     * Query is finishing (e.g. commit for autocommit queries)
     */
    FINISHING(false),
    /**
     * Query has finished executing and all output has been consumed.
     */
    FINISHED_EXECUTION(false),
    /**
     * Job has finished forwarding all output to S3/Hive
     */
    FINISHED(true),
    /**
     * Query was canceled by a user.
     */
    CANCELED(true),
    /**
     * Query execution failed.
     */
    FAILED(true);

    private final boolean doneState;

    JobState(boolean doneState)
    {
        this.doneState = doneState;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }

    public static Predicate<JobState> inDoneState()
    {
        return state -> state.isDone();
    }

    public static JobState fromStatementState(String statementState)
    {
        String state = statementState.equalsIgnoreCase("FINISHED") ? "FINISHED_EXECUTION" : statementState;
        return JobState.valueOf(state);
    }
}
