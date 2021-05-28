package org.apache.flink.streaming.controlplane.reconfigure.operator;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.controlplane.reconfigure.ControlFunctionManager;

public interface ControlFunction extends Function {

	/**
	 * Used to update operator logic.
	 * <p>
	 * Usage Example
	 * <p>
	 * Considering a word count application, there is a filter operator that will remove those words whose
	 * length is smaller than k1 (e.g. 10).
	 * In user's application, it may be defined by such code:
	 * <pre>{@code
	 * 	DataStream<String> wordStream = ... // something up data stream define here
	 *
	 * 	DataStream<String> filteredStream = wordStream.filter(
	 * 		new FilterFunction<String>() {
	 * 			@Override
	 * 			public boolean filter(String s) throws Exception {
	 * 				return s.length() > 10;
	 * 			}
	 * 		});
	 * 	// other code
	 * }</pre>
	 * <p>
	 * If we want to change to remove those words whose length is smaller than k2 (e.g. 100), we could invoke
	 * the "reconfigure" method of the {@link ControlFunctionManager} in such way:
	 * <pre>{@code
	 * 	ControlFunctionManager cf = ... // something getting ControlFunctionManager object here
	 * 	OperatorId filterOpID = ...	// something getting id of filter operator here
	 *
	 * 	cf.reconfigure(filterOpID,
	 * 		new ControlFunction(){
	 * 			@Override
	 * 			public void invokeControl(ControlContext ctx, Object input) {
	 * 				String inputWord = (String)input;
	 * 				if(inputWord.length() > 100){
	 * 					ctx.setCurrentRes(inputWord);
	 * 				}
	 * 			}
	 * 		});
	 * }</pre>
	 * <p>
	 * Then {@link ControlFunctionManager} will use the new ControlFunction embedded in the {@link ControlOperator}
	 * to replace the original filter operator
	 *
	 * @param ctx   the returning object will be stored in ctx collector
	 * @param input the input data
	 */
	void invokeControl(ControlContext ctx, Object input);

}
