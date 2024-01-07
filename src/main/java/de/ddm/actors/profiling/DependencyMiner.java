package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {
	// Colors for progress bar art
	public static final String ANSI_RED = "\u001B[31m";
	public static final String ANSI_GREEN = "\u001B[32m";
	public static final String ANSI_YELLOW = "\u001B[33m";
	public static final String ANSI_BLUE = "\u001B[34m";
	public static final String ANSI_PURPLE = "\u001B[35m";
	public static final String ANSI_CYAN = "\u001B[36m";
	public static final String ANSI_RESET = "\u001B[0m";
	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<String[]> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker; // Actor reference for communication.
		int taskID; // Identifier for the completed task.
		Content column1; // The content of the first column involved in the task.
		Content column2; // The content of the second column involved in the task.
		boolean dependency; // Result of the dependency check between the two columns.
	}

	// Represents a message to request specific column data in a distributed dependency checking task.
	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class getRequiredColumnMessage implements Message {
		ActorRef<LargeMessageProxy.Message> dependencyWorkerLargeMessageProxy; // Large message proxy for handling bulky data.
		private static final long serialVersionUID = -1025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker; // Actor reference for dependency worker communication.
		int taskId; // Identifier for the task requiring column data.
		String key1; // Key identifier for the first column.
		String key2; // Key identifier for the second column.
		boolean aBoolean; // A boolean flag, purpose varies based on context.
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
		this.dependencyWorkersLargeMessageProxy = new ArrayList<>();
		this.dependencyWorkers = new ArrayList<>();
		this.fileCounter = inputFiles.length;

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
		this.totalTasks = totalTasks;
		this.completedTasks = 0;
	}

	/////////////////
	// Actor State //
	/////////////////

	private long startTime;

	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private HashMap<String, Content> columnHashMap = new HashMap<>();

	private List<DependencyWorker.TaskMessage> taskMessageList = new ArrayList<>();
	private  List<ActorRef<LargeMessageProxy.Message>> dependencyWorkersLargeMessageProxy;
	private List<ActorRef<DependencyWorker.Message>> dependencyWorkers = new ArrayList<>();
	private int taskCounter = 0;
	private int fileCounter;
	private  int totalTasks;
	private int completedTasks;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(getRequiredColumnMessage.class,this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}


	// Handles messages requesting specific column data. Depending on the request, it retrieves column data and sends it back.

	private Behavior<Message> handle(getRequiredColumnMessage message) {
		if(message.aBoolean){  // If true, both columns are required.

			// Constructs a new ColumnMessage with content from both columns and sends it to the large message proxy.
			LargeMessageProxy.LargeMessage mess = new DependencyWorker.ColumnMessage(message.getTaskId(),this.columnHashMap.get(message.key1), message.getKey1(),
					this.columnHashMap.get(message.getKey2()), message.getKey2(),true);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(mess,message.dependencyWorkerLargeMessageProxy));


		} else {  // If false, only the first column is required.

			// Constructs a new ColumnMessage with content from the first column only and sends it to the large message proxy.
			LargeMessageProxy.LargeMessage mess = new DependencyWorker.ColumnMessage(message.getTaskId(),this.columnHashMap.get(message.key1), message.getKey1(),
					null, null,true);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(mess,message.dependencyWorkerLargeMessageProxy));

		}
		return this; // Returns the current behavior, as it's unmodified in this method.
	}


	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	// Processes a batch of rows from a file and requests the next batch or logs file completion.

	private Behavior<Message> handle(BatchMessage message) {
		String fileName = this.inputFiles[message.getId()].getName(); // Retrieves the file name associated with the batch.
		List<String[]> rows = message.getBatch(); // Extracts the batch of rows from the message.
		int rowCount = rows.size(); // Counts the number of rows in the batch.
		this.getContext().getLog().info("batches from file ==>  {}!", fileName); // Logs information about the current batch.

		if (!rows.isEmpty()) { // Checks if the batch contains any rows.
			processRows(message, rows); // Processes the rows if there are any.
			requestNextBatch(message.getId()); // Requests the next batch of rows from the file.
		} else {
			logFileCompletion(fileName); // Logs the completion of processing for the current file.
			checkAllFilesRead(); // Checks if all files have been read and processed.
		}

		return this; // Returns the current behavior, indicating no change in the actor's state.
	}


	// Iterates through each row of a batch, processing individual rows.
	private void processRows(BatchMessage message, List<String[]> rows) {
		for (String[] row : rows) {
			processRow(message, row);
		}
	}

	// Processes each row by adding column data to the corresponding content object in the columnHashMap.
	private void processRow(BatchMessage message, String[] row) {
		for (int columnNumber = 0; columnNumber < row.length; columnNumber++) {
			String columnName = this.headerLines[message.getId()][columnNumber];
			int finalColumnNumber = columnNumber;
			columnHashMap.computeIfAbsent(columnName, k -> new Content(finalColumnNumber, columnName, this.inputFiles[message.getId()].getName()))
					.add(row[columnNumber]);
		}
	}

	// Requests the next batch of rows from the InputReader actor for a specific file.
	private void requestNextBatch(int fileId) {
		this.inputReaders.get(fileId).tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
	}

	// Logs the completion of file processing.
	private void logFileCompletion(String fileName) {
		this.getContext().getLog().info("Reading file {} is finished", fileName);
	}

	// Lines of ASCII art and colors are predefined.
	// Iterates through each line, applying a color and printing it.
	private void printRainbowText() {
		String[] asciiArtLines = {
				"________          __              _____                      .__                         ",
				"\\______ \\ _____ _/  |______      /     \\  __ __  ____   ____ |  |__   ___________  ______",
				" |    |  \\\\__  \\\\   __\\__  \\    /  \\ /  \\|  |  \\/    \\_/ ___\\|  |  \\_/ __ \\_  __ \\/  ___/",
				" |    `   \\/ __ \\|  |  / __ \\_ /    Y    \\  |  /   |  \\  \\___|   Y  \\  ___/|  | \\/\\___ \\ ",
				"/_______  (____  /__| (____  / \\____|__  /____/|___|  /\\___  >___|  /\\___  >__|  /____  >",
				"        \\/     \\/          \\/          \\/           \\/     \\/     \\/     \\/           \\/ "
		};

		String[] colors = {ANSI_RED, ANSI_YELLOW, ANSI_GREEN, ANSI_CYAN, ANSI_BLUE, ANSI_PURPLE};

		for (int i = 0; i < asciiArtLines.length; i++) {
			String coloredLine = colors[i % colors.length] + asciiArtLines[i] + ANSI_RESET;
			System.out.println(coloredLine);
		}
	}


	// Decrements the file counter and checks if all files are processed. If so, logs the completion and performs additional checks.
	private void checkAllFilesRead() {
		fileCounter--;
		if (fileCounter == 0) {
			this.getContext().getLog().info("All files have been read");
			printRainbowText(); // Prints a colorful completion message.

			Checking(); // Performs additional checks or actions after all files are read.
		}
	}


	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			this.dependencyWorkersLargeMessageProxy.add(message.getDependencyWorkerLargeMessageProxy());
		}
		return this;
	}

	private Behavior<Message> handle(CompletionMessage msg) {

		ActorRef<DependencyWorker.Message> workerRef = msg.getDependencyWorker();

		if (msg.isDependency()) {
			InclusionDependency indObj = constructInclusionDependency(msg);
			dispatchResults(indObj);
		}
		reassignTasks(workerRef);

		return this;
	}

	// Constructs an InclusionDependency object based on completion message details.
	private InclusionDependency constructInclusionDependency(CompletionMessage msg) {
		File depFile = new File(msg.getColumn2().getName());
		File refFile = new File(msg.getColumn1().getName());
		String[] depAttributes = {msg.getColumn2().getColumnName()};
		String[] refAttributes = {msg.getColumn1().getColumnName()};

		return new InclusionDependency(depFile, depAttributes, refFile, refAttributes);
	}

	// Dispatches the constructed InclusionDependency object to the result collector.
	private void dispatchResults(InclusionDependency indObj) {
		List<InclusionDependency> indList = new ArrayList<>(1);
		indList.add(indObj);
		this.resultCollector.tell(new ResultCollector.ResultMessage(indList));
	}

	// Initiates the generation and distribution of tasks to workers.
	private void Checking() {
		generateTasksForWorkers();
		distributeTasksToWorkers();
	}

	// Generates tasks for all unique pairs of column keys.
	private void generateTasksForWorkers() {
		for (String key1 : columnHashMap.keySet()) {
			for (String key2 : columnHashMap.keySet()) {
				if (!key1.equals(key2)) {
					createAndAddTask(key1, key2);
				}
			}
		}
	}

	// Creates a new task for a pair of column keys and adds it to the task list.
	private void createAndAddTask(String key1, String key2) {
		DependencyWorker.TaskMessage task = new DependencyWorker.TaskMessage(this.largeMessageProxy, -1, key1, key2);
		taskMessageList.add(task);
	}

	// Distributes the tasks to available workers.
	private void distributeTasksToWorkers() {
		for (ActorRef<DependencyWorker.Message> worker : dependencyWorkers) {
			reassignTasks(worker);
		}
	}

	// Reassigns tasks to workers and updates the progress bar.
	private void reassignTasks(ActorRef<DependencyWorker.Message> dependencyWorker) {
		updateProgressBar();

		if (checkRemainingTasks()) {
			DependencyWorker.TaskMessage taskMessage = this.taskMessageList.get(taskCounter);
			taskMessage.setDependencyMinerLargeMessageProxy(this.largeMessageProxy);
			this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(taskMessage, this.dependencyWorkersLargeMessageProxy.get(this.dependencyWorkers.indexOf(dependencyWorker))));
			taskCounter++;
		} else {
			this.getContext().getLog().info("All tasks are given to Workers");
			this.end();
		}
	}

	// State variable for spinner animation in progress bar.
	private int spinnerState = 0;

	// Updates the progress bar showing task completion status.
	private void updateProgressBar() {
		double progressPercentage = (double) taskCounter / taskMessageList.size();
		int barWidth = 70;
		StringBuilder bar = new StringBuilder("[");

		String colorStart = "\u001B[32m"; // Green color code.
		String colorEnd = "\u001B[0m";     // Reset color code.

		int filledLength = (int)(barWidth * progressPercentage);
		for (int i = 0; i < barWidth; i++) {
			if (i < filledLength) {
				bar.append(colorStart).append("=").append(colorEnd);
			} else if (i == filledLength) {
				bar.append(getSpinnerCharacter());
			} else {
				bar.append(" ");
			}
		}

		bar.append("] ")
				.append(String.format("%6.2f%%", progressPercentage * 100));

		System.out.print("\r" + bar.toString());
		if (taskCounter == taskMessageList.size()) {
			System.out.println();
		}
	}

	// Returns a character for spinner animation based on the current state.
	private char getSpinnerCharacter() {
		char[] spinnerChars = {'|', '/', '-', '\\'};
		char spinnerChar = spinnerChars[spinnerState % spinnerChars.length];
		spinnerState++;
		return spinnerChar;
	}

	// Checks if there are remaining tasks to be assigned.
	private boolean checkRemainingTasks() {
		return taskCounter < taskMessageList.size();
	}



	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;

		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}