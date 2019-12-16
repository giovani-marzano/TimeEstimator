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

open class BaseTaskVertex(
    id: String,
    type: VertexTypes,
    val taskId: String = "",
    val name: String = ""
) : BaseVertex(id, type) {
    var priority: Int = 0

    override fun fieldsString(): String {
        return "${super.fieldsString()}, taskId: $taskId, name: $name, priority: $priority"
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
) :
    BaseTaskVertex(id, VertexTypes.TASK, taskId, name) {
    override fun fieldsString(): String {
        return "${super.fieldsString()}, points: $points, status; $status"
    }
}

class WorkerVertex(id: String, val dedication: Double = 1.0) : BaseVertex(id, VertexTypes.WORKER)

val taskPriorityComparator = Comparator<BaseVertex> { t1, t2 ->
    val p1 = (t1 as? BaseTaskVertex)?.let { it.priority } ?: 0
    val p2 = (t2 as? BaseTaskVertex)?.let { it.priority } ?: 0
    p1 - p2
}
