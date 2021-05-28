package org.apache.flink.runtime.rescale.reconfigure;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.Task;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class TaskOperatorManager {

	public final static int NEED_SYNC_REQUEST = -1;
	public final static int NEED_RESUME_REQUEST = -2;

	private final PauseActionController pauseActionController;
	private final Task containedTask;
	private final AtomicLong hasSyncRequest = new AtomicLong(0L);

	public TaskOperatorManager(Task task) {
		this.containedTask = task;
		this.pauseActionController = new PauseActionControllerImpl(containedTask.getTaskInfo().getTaskNameWithSubtasks());
	}

	public PauseActionController getPauseActionController() {
		return pauseActionController;
	}

	public void setSyncRequestFlag(int syncFlag) throws Exception {
		switch (syncFlag){
			case NEED_SYNC_REQUEST:
				System.out.println(containedTask.getTaskInfo().getTaskNameWithSubtasks() + ": prepare to synchronize");
				break;
			case NEED_RESUME_REQUEST:
				System.out.println(containedTask.getTaskInfo().getTaskNameWithSubtasks() + ": task resuming...");
				this.getPauseActionController().resume();
				break;
			default:
				throw new Exception(containedTask.getTaskInfo().getTaskNameWithSubtasks() + ": unknown flag");
		}
		hasSyncRequest.set(syncFlag);
	}

	public boolean acknowledgeSyncRequest(long finishedSyncRequestID){
		return hasSyncRequest.compareAndSet(NEED_SYNC_REQUEST, finishedSyncRequestID);
	}

	@ThreadSafe
	public interface PauseActionController {

		/**
		 * This method should be thread safe.
		 * called by task process thread, should avoid acquire lock as possible as we can
		 * since task will check this method each time process an input.
		 *
		 * @return true if task should be paused
		 */
		boolean ackIfPause();

		CompletableFuture<Acknowledge> getResumeFuture();

		CompletableFuture<Acknowledge> getAckPausedFuture();

		/**
		 * This method should be thread safe.
		 * called by none task process thread
		 *
		 * @return A future to wait for acknowledgment of task process thread
		 * @throws Exception
		 */
		CompletableFuture<Acknowledge> setPausedAndGetAckFuture() throws Exception;

		void resume() throws Exception;
	}

	public static class PauseActionControllerImpl implements PauseActionController {

		private CompletableFuture<Acknowledge> resumeFuture;
		private CompletableFuture<Acknowledge> ackPausedFuture;
		private final AtomicReference<TaskStatus> state = new AtomicReference<>(TaskStatus.READY);

		private final Object lock = new Object();

		private final String taskName;

		public PauseActionControllerImpl(String taskName) {
			this.taskName = taskName;
		}

		@Override
		public boolean ackIfPause() {
			if (state.get() == TaskStatus.PAUSE) {
				// only the state become pause should we acquire the lock
				System.out.println(taskName + ": suspend process to wait for pauseActionController ready");
				synchronized (lock) {
					ackPausedFuture.complete(Acknowledge.get());
					return true;
				}
			}
			return false;
		}

		@Override
		public void resume() throws Exception {
			if (state.compareAndSet(TaskStatus.PAUSE, TaskStatus.READY)) {
				resumeFuture.complete(Acknowledge.get());
			}
		}

		@Override
		public CompletableFuture<Acknowledge> setPausedAndGetAckFuture() throws Exception {
			synchronized (lock) {
				if (state.compareAndSet(TaskStatus.READY, TaskStatus.PAUSE)) {
					ackPausedFuture = new CompletableFuture<>();
					resumeFuture = new CompletableFuture<>();
					return ackPausedFuture;
				} else {
					throw new Exception("state has been paused before");
				}
			}
		}

		@Override
		public CompletableFuture<Acknowledge> getResumeFuture() {
			return resumeFuture;
		}

		@Override
		public CompletableFuture<Acknowledge> getAckPausedFuture() {
			if (state.get() != TaskStatus.PAUSE) {
				return CompletableFuture.completedFuture(Acknowledge.get());
			}
			return ackPausedFuture;
		}

		private enum TaskStatus {
			PAUSE,
			READY
		}
	}

	@VisibleForTesting
	public static class NoControlImpl implements PauseActionController {

		@Override
		public boolean ackIfPause() {
			return false;
		}

		@Override
		public CompletableFuture<Acknowledge> getResumeFuture() {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		@Override
		public CompletableFuture<Acknowledge> getAckPausedFuture() {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		@Override
		public CompletableFuture<Acknowledge> setPausedAndGetAckFuture() throws Exception {
			return CompletableFuture.completedFuture(Acknowledge.get());
		}

		@Override
		public void resume() throws Exception {

		}
	}

}
