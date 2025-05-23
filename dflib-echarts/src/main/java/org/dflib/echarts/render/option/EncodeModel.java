package org.dflib.echarts.render.option;

import org.dflib.echarts.render.ValueModels;

public class EncodeModel {

    private final Integer x;
    private final ValueModels<Integer> ys;
    private final Integer value;
    private final Integer itemName;

    public EncodeModel(Integer x, ValueModels<Integer> ys, Integer itemName, Integer value) {
        this.itemName = itemName;
        this.x = x;
        this.ys = ys;
        this.value = value;
    }

    public Integer getX() {
        return x;
    }

    public boolean isYsPresent() {
        return ys != null && ys.size() > 1;
    }

    public ValueModels<Integer> getYs() {
        return ys;
    }

    public boolean isYPresent() {
        return ys != null && ys.size() == 1;
    }

    public Integer getY() {
        return ys.getValue(0);
    }

    public Integer getItemName() {
        return itemName;
    }

    public Integer getValue() {
        return value;
    }
}
