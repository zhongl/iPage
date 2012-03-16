/*
 * Copyright 2012 zhongl
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.github.zhongl.api;

import org.softee.management.annotation.Description;
import org.softee.management.annotation.MBean;
import org.softee.management.annotation.ManagedAttribute;

import static java.lang.Math.max;
import static java.lang.Math.min;

/** @author <a href="mailto:zhong.lunfu@gmail.com">zhongl<a> */
@MBean
class DefragPolicy {
    private volatile int lastAliveSize;
    private volatile long increment;
    private volatile long decrement;

    private volatile int availableFreeMemoryRatio;
    private volatile int gapRatioThreshold;
    private volatile int aliveIndexOccupied;

    DefragPolicy() {
        setAliveIndexOccupied(40);
        setAvailableFreeMemoryRatio(7);
        setGapRatioThreshold(2);
    }

    public boolean evaluate(final int aliveSize, final int probableDelta) {
        int delta = aliveSize - lastAliveSize;
        if (delta > 0) increment += delta;
        else decrement -= delta;

        this.lastAliveSize = aliveSize;

        double availableMemoryForCollectAliveIndex = Runtime.getRuntime().freeMemory() * availableFreeMemoryRatio * 0.1;

        boolean needDefrag = deltaRatio() <= gapRatioThreshold * 0.1
                && availableMemoryForCollectAliveIndex >= (aliveSize + probableDelta) * aliveIndexOccupied;

        if (needDefrag) {
            increment = aliveSize;
            decrement = 0;
        }

        return needDefrag;
    }

    @ManagedAttribute
    public int getLastAliveSize() {
        return lastAliveSize;
    }

    @ManagedAttribute
    public long getIncrement() {
        return increment;
    }

    @ManagedAttribute
    public long getDecrement() {
        return decrement;
    }

    @ManagedAttribute
    public int getAvailableFreeMemoryRatio() {
        return availableFreeMemoryRatio;
    }

    @ManagedAttribute
    @Description("Available free memory ratio for defrag")
    public void setAvailableFreeMemoryRatio(@Description("Ratio in [1, 9]") int availableFreeMemoryRatio) {
        this.availableFreeMemoryRatio = max(1, min(9, availableFreeMemoryRatio));
    }

    @ManagedAttribute
    public int getAliveIndexOccupied() {
        return aliveIndexOccupied;
    }

    @ManagedAttribute
    @Description("An alive index occupied for memory usage evaluation")
    public void setAliveIndexOccupied(
            @Description("Occupied bytes in [32, Integer.MAX_VALUE]")
            int aliveIndexOccupied) {
        this.aliveIndexOccupied = max(32, aliveIndexOccupied);
    }

    @ManagedAttribute
    public int getGapRatioThreshold() {
        return gapRatioThreshold;
    }

    @ManagedAttribute
    public void setGapRatioThreshold(
            @Description("Gap ratio threshold in [1, 9]")
            int gapRatioThreshold) {
        this.gapRatioThreshold = max(1, min(9, gapRatioThreshold));
    }

    private double deltaRatio() {
        long gap = increment - decrement;
        if (gap != 0) return gap * 1.0 / increment;
        increment = decrement = 0;
        return 0;
    }

}
