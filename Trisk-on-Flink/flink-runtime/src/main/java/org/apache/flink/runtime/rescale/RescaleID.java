package org.apache.flink.runtime.rescale;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.Serializable;

public class RescaleID implements Serializable {

	private static final long serialVersionUID = -1588738478594839245L;

	private final long rescaleId;

	private static long counter;

	private RescaleID(long id) {
		rescaleId = id;
	}

	public void writeTo(ByteBuf buf) {
		buf.writeLong(rescaleId);
	}

	public static RescaleID fromByteBuf(ByteBuf buf) {
		long rescaleId = buf.readLong();
		return new RescaleID(rescaleId);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == getClass()) {
			RescaleID other = (RescaleID) obj;
			return this.rescaleId == other.rescaleId;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return (int) (this.rescaleId + 1);
	}

	@Override
	public String toString() {
		return String.valueOf(rescaleId);
	}

	// next ID start from 1
	public static RescaleID generateNextID() {
		return new RescaleID(++counter);
	}

	public static RescaleID DEFAULT = new RescaleID(0);
}
