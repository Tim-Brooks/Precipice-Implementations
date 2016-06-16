package net.uncontended.precipice_implementations.asynchttp;

import net.uncontended.precipice.Failable;

public enum HTTPResult implements Failable {

    SUCCESS(false),
    TIMEOUT(true),
    ERROR(true);

    private final boolean isFailure;

    private HTTPResult(boolean isFailure) {
        this.isFailure = isFailure;
    }

    @Override
    public boolean isFailure() {
        return isFailure;
    }

    @Override
    public boolean isSuccess() {
        return !isFailure;
    }
}
