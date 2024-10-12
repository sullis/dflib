package org.dflib.echarts;

import org.dflib.echarts.render.option.Distance;
import org.dflib.echarts.render.option.GridModel;

import java.util.Objects;

public class Grid {

    private LeftDistance left;
    private Distance right;
    private TopDistance top;
    private Distance bottom;
    private Distance width;
    private Distance height;

    protected Grid() {
    }

    public static Grid of() {
        return new Grid();
    }

    public Grid leftPct(double pct) {
        this.left = new LeftDistance(Distance.ofPct(pct));
        return this;
    }

    public Grid leftPx(int pixels) {
        this.left = new LeftDistance(Distance.ofPx(pixels));
        return this;
    }

    public Grid leftLeft() {
        this.left = new LeftDistance(AutoLeftDistance.left);
        return this;
    }

    public Grid leftRight() {
        this.left = new LeftDistance(AutoLeftDistance.right);
        return this;
    }

    public Grid leftCenter() {
        this.left = new LeftDistance(AutoLeftDistance.center);
        return this;
    }

    public Grid rightPct(double pct) {
        this.right = Distance.ofPct(pct);
        return this;
    }

    public Grid rightPx(int pixels) {
        this.right = Distance.ofPx(pixels);
        return this;
    }

    public Grid topPct(double pct) {
        this.top = new TopDistance(Distance.ofPct(pct));
        return this;
    }

    public Grid topPx(int pixels) {
        this.top = new TopDistance(Distance.ofPx(pixels));
        return this;
    }

    public Grid topTop() {
        this.top = new TopDistance(AutoTopDistance.top);
        return this;
    }

    public Grid topMiddle() {
        this.top = new TopDistance(AutoTopDistance.middle);
        return this;
    }

    public Grid topBottom() {
        this.top = new TopDistance(AutoTopDistance.bottom);
        return this;
    }

    public Grid bottomPct(double pct) {
        this.bottom = Distance.ofPct(pct);
        return this;
    }

    public Grid bottomPx(int pixels) {
        this.bottom = Distance.ofPx(pixels);
        return this;
    }

    public Grid widthPct(double pct) {
        this.width = Distance.ofPct(pct);
        return this;
    }

    public Grid widthPx(int pixels) {
        this.width = Distance.ofPx(pixels);
        return this;
    }

    public Grid heightPct(double pct) {
        this.height = Distance.ofPct(pct);
        return this;
    }

    public Grid heightPx(int pixels) {
        this.height = Distance.ofPx(pixels);
        return this;
    }

    protected GridModel resolve() {
        return new GridModel(
                left != null ? left.asString() : null,
                right != null ? right.asString() : null,
                top != null ? top.asString() : null,
                bottom != null ? bottom.asString() : null,
                width != null ? width.asString() : null, // ignoring 'auto' option, which is the default
                height != null ? height.asString() : null // ignoring 'auto' option, which is the default
        );
    }

    static class LeftDistance {
        private final Distance distance;
        private final AutoLeftDistance autoDistance;

        LeftDistance(AutoLeftDistance autoDistance) {
            this.autoDistance = Objects.requireNonNull(autoDistance);
            this.distance = null;
        }

        LeftDistance(Distance distance) {
            this.distance = Objects.requireNonNull(distance);
            this.autoDistance = null;
        }

        String asString() {
            return distance != null ? distance.asString() : "'" + autoDistance.name() + "'";
        }
    }

    enum AutoLeftDistance {
        left, center, right
    }

    static class TopDistance {
        private final Distance distance;
        private final AutoTopDistance autoDistance;

        TopDistance(AutoTopDistance autoDistance) {
            this.autoDistance = Objects.requireNonNull(autoDistance);
            this.distance = null;
        }

        TopDistance(Distance distance) {
            this.distance = Objects.requireNonNull(distance);
            this.autoDistance = null;
        }

        String asString() {
            return distance != null ? distance.asString() : "'" + autoDistance.name() + "'";
        }
    }

    enum AutoTopDistance {
        top, middle, bottom
    }
}
