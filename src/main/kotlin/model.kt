package timeestimator

import java.util.Comparator

enum class VertexTypes {
    NONE, GOAL, MARK, TASK, STAFF, WORKER
}

enum class TaskStatus {
    NONE, PLANNING, PENDING, DOING, DONE
}

open class BaseVertex(val id: String, val type: VertexTypes) {
    override fun hashCode(): Int {
        return id.hashCode();
    }

    override fun equals(other: Any?) = when (other) {
        is BaseVertex -> {
            id == other.id
        }
        else -> false
    }

    open fun fieldsString(): String {
        return "id: $id, type: $type"
    }

    override fun toString(): String {
        return "(${fieldsString()})"
    }
}

open class PrioritizedVertex(id: String, type: VertexTypes) : BaseVertex(id, type) {
    var priority: Int = 0

    override fun fieldsString(): String {
        return "${super.fieldsString()}, priority: $priority"
    }
}

open class BaseTaskVertex(
    id: String,
    type: VertexTypes,
    val taskId: String = "",
    val name: String = ""
) : PrioritizedVertex(id, type) {
    override fun fieldsString(): String {
        return "${super.fieldsString()}, taskId: $taskId, name: $name"
    }
}

class MarkVertex(id: String, taskId: String = "", name: String = "") :
    BaseTaskVertex(id, VertexTypes.MARK, taskId, name);

class TaskVertex(
    id: String,
    taskId: String = "",
    name: String = "",
    val points: Double = 1.0,
    var status: TaskStatus = TaskStatus.PENDING
) : BaseTaskVertex(id, VertexTypes.TASK, taskId, name) {
    override fun fieldsString(): String {
        return "${super.fieldsString()}, points: $points, status; $status"
    }
}

class WorkerVertex(id: String, val dedication: Double = 1.0) : BaseVertex(id, VertexTypes.WORKER)

val taskPriorityComparator = Comparator.comparing<BaseTaskVertex, Int> { it.priority }
