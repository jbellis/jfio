package io.github.jbellis.jfio;

abstract class Submissions {
    private final int maxInFlight;
    private int inFlight;

    /**
     * Submissions that are either pending (in {@code segment}, or are "in flight" (submitted but whose completion
     * has not yet been acknowledged). The index of the submission in this list is its "id".
     */
    private final Submission[] inFlightOrPending;
    private int lastSubmittedIndex = -1;

    protected final int maxPending;
    private int pending;

    Submissions(int depth) {
        this.maxPending = depth;
        this.maxInFlight = depth * 2;
        this.inFlightOrPending = new Submission[maxInFlight];
    }

    private int assignId(Submission submission) {
        for (int i = 0; i < maxInFlight; i++) {
            lastSubmittedIndex = (lastSubmittedIndex + 1) % maxInFlight;
            if (inFlightOrPending[lastSubmittedIndex] == null) {
                inFlightOrPending[lastSubmittedIndex] = submission;
                return lastSubmittedIndex;
            }
        }
        throw new IllegalStateException("Couldn't acquire an ID");
    }

    int pending() {
        return this.pending;
    }

    int inFlight() {
        return inFlight;
    }

    int maxInFlight() {
        return maxInFlight;
    }

    int room() {
        return this.maxPending - this.pending;
    }

    boolean add(Submission submission) {
        if (this.pending == this.maxPending) {
            return false;
        }

        int id = assignId(submission);
        addSubmissionInternal(this.pending++, id, submission);
        return true;
    }

    void onSubmitted(int count) {
        if (count == 0) {
            return;
        }

        inFlight += count;
        if (count >= this.pending) {
            this.pending = 0;
        } else {
            int remaining = this.pending - count;
            for (int i = 0; i < remaining; i++) {
                move(count + i, i);
            }
            this.pending = remaining;
        }
    }

    void onCompleted(int id, int res) {
        inFlight--;
        Submission submission = inFlightOrPending[id];
        assert submission != null;
        submission.onCompletion(res);
        inFlightOrPending[id] = null;
    }

    abstract void addSubmissionInternal(int index, int id, Submission submission);
    abstract void move(int from, int to);
    abstract int idOfSubmission(int index);

    @Override
    public String toString() {
        if (this.pending == 0) {
            return "[]";
        }

        StringBuilder sb = new StringBuilder();
        sb.append("[\n");
        for (int i = 0; i < pending; i++) {
            if (i > 0) {
                sb.append("\n");
            }
            int id = idOfSubmission(i);
            sb.append("  ").append(id).append(": ").append(inFlightOrPending[id]);
        }
        sb.append("\n]");
        return sb.toString();
    }
}
