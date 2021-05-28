package org.apache.flink.streaming.controlplane.rescale.streamswitch;

import java.util.ArrayList;
import java.util.List;

/**
 * this class is only for stream switch
 */
public class RescaleActionDescriptor {

	List<BaseRescaleAction> rescaleActionList;

	private RescaleActionDescriptor() {
		rescaleActionList = new ArrayList<>();
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("rescale: (");
		for (BaseRescaleAction action : rescaleActionList) {
			sb.append(action.toString()).append(", ");
		}
		sb.delete(sb.length() - 2, sb.length());
		return sb.append(')').toString();
	}

	private void thenScaleIn(int newParallelism) {
		this.rescaleActionList.add(new SimpleRescaleAction(BaseRescaleAction.ActionType.SCALE_IN, newParallelism));
	}


	private void thenScaleOut(int newParallelism) {
		this.rescaleActionList.add(new SimpleRescaleAction(BaseRescaleAction.ActionType.SCALE_OUT, newParallelism));
	}

	private void thenPartition() {
		this.rescaleActionList.add(new SimpleRescaleAction(BaseRescaleAction.ActionType.REPARTITION, 0));
	}

	private void thenCombine(RescaleActionDescriptor descriptor) {
		this.rescaleActionList.add(new ListRascalAction(descriptor.rescaleActionList));
	}

	private static final String SCALE_IN = "in";
	private static final String SCALE_OUT = "out";
	private static final String REPARTITION = "repartition";

	/**
	 * The input should not contains any prefix and posfix such as "rescale", "(", ")"
	 * we don't support second inner action new, may be support in future.
	 * <p>
	 * This type of action list is not support
	 * "rescale: (((repartition), repartition), repartition)"
	 *
	 * @param description
	 * @return
	 */
	public static RescaleActionDescriptor decode(String description) throws Exception {
		//remove first "rescale: (" and last ")"
		if (description.startsWith("rescale") || description.startsWith("(")) {
			throw new Exception("remove prefix and postfix first");
		}
		if (description.contains("(")) {
			throw new Exception("do not support list action now, will support in future");
		}
		String[] actions = description.split(", ");

		RescaleActionDescriptor descriptor = new RescaleActionDescriptor();
		for (String action : actions) {
			String[] type = action.split(": ");
			switch (type[0]) {
				case SCALE_IN:
					descriptor.thenScaleIn(Integer.parseInt(type[1]));
					break;
				case SCALE_OUT:
					descriptor.thenScaleOut(Integer.parseInt(type[1]));
					break;
				case REPARTITION:
					descriptor.thenPartition();
					break;
				default:
					// it is a list action, currently show not attached here
					throw new Exception("unknown action type: " + action);
//                    descriptor.thenCombine(decode(action.substring(1, action.length()-1)));
			}
		}
		return descriptor;
	}


	static abstract class BaseRescaleAction {
		final ActionType actionType;

		BaseRescaleAction(ActionType type) {
			this.actionType = type;
		}

		public enum ActionType {

			SCALE_IN("in"),
			SCALE_OUT("out"),
			REPARTITION("repartition"),
			COMBINE_ALL("all");

			final String identifier;

			ActionType(String identifier) {
				this.identifier = identifier;
			}
		}
	}

	public static class SimpleRescaleAction extends BaseRescaleAction {
		final int newParallelism;

		SimpleRescaleAction(ActionType type, int newParallelism) {
			super(type);
			this.newParallelism = newParallelism;
		}

		@Override
		public String toString() {
			if (super.actionType == ActionType.REPARTITION) {
				return actionType.identifier;
			}
			return super.actionType.identifier + ": " + newParallelism;
		}
	}

	static class ListRascalAction extends BaseRescaleAction {
		private final List<BaseRescaleAction> rescaleActionList;

		ListRascalAction(List<BaseRescaleAction> rescaleActionList) {
			super(ActionType.COMBINE_ALL);
			this.rescaleActionList = rescaleActionList;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append('(');
			for (BaseRescaleAction action : rescaleActionList) {
				sb.append(action.toString()).append(", ");
			}
			sb.delete(sb.length() - 2, sb.length());
			return sb.append(')').toString();
		}
	}

}
