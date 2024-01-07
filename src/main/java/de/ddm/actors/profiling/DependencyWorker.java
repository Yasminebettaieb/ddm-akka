package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.HashMap;
import java.util.Set;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@Setter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message,LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		int task;
		String firstElementId;
		String secondElementId;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ColumnMessage implements Message,LargeMessageProxy.LargeMessage {
		private static final long serialVersionUID = -5667745204456518160L;
		int taskId;
		Content firtColumnContent;
		String firstElementKey;
		Content secondColumnContent;
		String secondElementKey;
		boolean areBothColumnsMissing;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////


	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;
	private final HashMap<String, Content> columnContentMap = new HashMap<>();
	private TaskMessage currentTaskMessage;
	private String firstElementKey;
	private String secondElementKey;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.onMessage(ColumnMessage.class,this::handle)
				.build();
	}
	// Handles incoming ColumnMessage by evaluating inclusion dependencies and updating the column content map.
	private Behavior<Message> handle(ColumnMessage message) {
		evaluateInclusionDependency(); // Checks for inclusion dependencies based on current task.
		boolean hasSecondElement = message.getSecondElementKey() != null; // Checks if there is a second element.
		columnContentMap.putIfAbsent(message.getFirstElementKey(), message.getFirtColumnContent()); // Updates first element content.
		if (hasSecondElement) {
			columnContentMap.putIfAbsent(message.getSecondElementKey(), message.getSecondColumnContent()); // Updates second element content if exists.
		}
		return this; // Returns the current behavior.
	}

	// Evaluates whether an inclusion dependency exists between two columns.
	private void evaluateInclusionDependency() {
		boolean dependencyExists = false; // Initially assumes no dependency.

		// Retrieves the content for both columns from the current task message.
		Content firstColumnData = columnContentMap.get(currentTaskMessage.getFirstElementId());
		Content secondColumnData = columnContentMap.get(currentTaskMessage.getSecondElementId());

		// Checks for non-null content and whether the first column's content includes all of the second column's content.
		if (firstColumnData != null && secondColumnData != null) {
			dependencyExists = firstColumnData.getContent().containsAll(secondColumnData.getContent());
		}

		// Constructs a completion message with the dependency result and sends it to the large message proxy for further handling.
		LargeMessageProxy.LargeMessage dependencyResultMessage = new DependencyMiner.CompletionMessage(
				this.getContext().getSelf(), currentTaskMessage.getTask(), firstColumnData, secondColumnData, dependencyExists);
		largeMessageProxy.tell(new LargeMessageProxy.SendMessage(dependencyResultMessage, currentTaskMessage.getDependencyMinerLargeMessageProxy()));
	}



	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf(),this.largeMessageProxy));
		return this;
	}

	// Handles TaskMessage by updating current task, checking column requirements, and processing accordingly.
	private Behavior<Message> handle(TaskMessage msg) {
		this.currentTaskMessage = msg; // Updates the current task message with the incoming message.
		String firstElement = msg.firstElementId; // The ID of the first element involved in the task.
		String secondElement = msg.secondElementId; // The ID of the second element involved in the task.

		// Determines if both or either of the columns' data is needed.
		boolean needBothColumns = !columnContentMap.containsKey(firstElement) && !columnContentMap.containsKey(secondElement);
		boolean needFirstColumn = !columnContentMap.containsKey(firstElement);
		boolean needSecondColumn = !columnContentMap.containsKey(secondElement);

		// Sends column requests based on the need determined above or evaluates dependency directly if all data is present.
		if (needBothColumns) {
			sendColumnRequest(msg, true, firstElement, secondElement);
		} else if (needFirstColumn) {
			sendColumnRequest(msg, false, firstElement, null);
		} else if (needSecondColumn) {
			sendColumnRequest(msg, false, secondElement, null);
		} else {
			evaluateInclusionDependency(); // All required data is available; evaluate inclusion dependency.
		}

		return this; // Returns the current behavior.
	}

	// Constructs and sends a column request message to the LargeMessageProxy based on the task details.
	private void sendColumnRequest(TaskMessage task, boolean bothColumns, String col1, String col2) {
		// Creates a getRequiredColumnMessage with details of required columns.
		LargeMessageProxy.LargeMessage reqMsg = new DependencyMiner.getRequiredColumnMessage(this.largeMessageProxy,
				this.getContext().getSelf(), task.getTask(), col1, col2, bothColumns);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(reqMsg, task.getDependencyMinerLargeMessageProxy())); // Sends the message.
	}

}