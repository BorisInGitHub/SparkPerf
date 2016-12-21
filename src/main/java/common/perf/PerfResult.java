package common.perf;

/**
 * Created by breynard on 26/10/16.
 */
public class PerfResult {
    private final long preparationDuration;
    private final long searchDuration;
    private final long countDuration;

    public PerfResult(long preparationDuration, long searchDuration, long countDuration) {
        super();
        this.preparationDuration = preparationDuration;
        this.searchDuration = searchDuration;
        this.countDuration = countDuration;
    }

    @Override
    public String toString() {
        if (countDuration == -1) {
            return "\tpreparationDuration=" + preparationDuration + "ms\t searchDuration=" + searchDuration + "ms\t countDuration=Not Applicable";
        } else {
            return "\tpreparationDuration=" + preparationDuration + "ms\t searchDuration=" + searchDuration + "ms\t countDuration=" + countDuration + "ms";
        }
    }
}
