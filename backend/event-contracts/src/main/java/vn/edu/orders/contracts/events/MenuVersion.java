package vn.edu.orders.contracts.events;

public record MenuVersion(int epoch, long seq) implements Comparable<MenuVersion> {
    @Override
    public int compareTo(MenuVersion other) {
        int epochCmp = Integer.compare(this.epoch, other.epoch);
        if (epochCmp != 0) {
            return epochCmp;
        }
        return Long.compare(this.seq, other.seq);
    }

    public boolean isNewerThan(MenuVersion other) {
        return other == null || compareTo(other) > 0;
    }
}
