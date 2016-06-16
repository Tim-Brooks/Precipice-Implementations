package net.uncontended.precipice_implementations.asynchttp;

import net.uncontended.precipice.Failable;

public enum HTTPResult implements Failable {

    SUCCESS,
    TIMEOUT,
    ERROR;

    @Override
    public boolean isFailure() {
        return false;
    }

    @Override
    public boolean isSuccess() {
        return false;
    }
}
