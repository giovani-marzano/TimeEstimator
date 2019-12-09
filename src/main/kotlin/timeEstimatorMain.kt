package marzano.giovani.timeestimator

import org.jgrapht.graph.DefaultEdge
import org.jgrapht.io.*
import kotlin.Exception

fun main(args: Array<String>) {
    println("Hello world")
}

fun createGraphMLImporter(): GraphImporter<BaseVertex, DefaultEdge> {
    val vertexProvider = VertexProvider<BaseVertex> {
        id: String, attributes: Map<String, Attribute> ->
        BaseVertex(id)
    }

    val edgeProvider = EdgeProvider<BaseVertex, DefaultEdge> {
        from: BaseVertex , to: BaseVertex, label: String, attributes: Map<String, Attribute> ->
        DefaultEdge()
    }

    return GraphMLImporter(vertexProvider, edgeProvider);
}

fun vertexProvider(id: String, attributes: Map<String, Attribute>): BaseVertex {
    return when (extractVertexType(id, attributes)) {
        VertexTypes.GOAL -> GoalVertex(id)
        VertexTypes.MARK -> MarkVertex(id)
        VertexTypes.TASK -> TODO()
        VertexTypes.STAFF -> StaffVertex(id)
        VertexTypes.WORKER -> TODO()
    }
}

private fun extractVertexType(
    id: String,
    attributes: Map<String, Attribute>
): VertexTypes {
    val typeStr = attributes["type"]?.value?.toUpperCase() ?: throw Exception("Vertex $id has no 'type'")
    return try {
        VertexTypes.valueOf(typeStr)
    } catch (ex: IllegalArgumentException) {
        throw Exception("Vertex $id has invalid type '$typeStr'", ex)
    }
}

enum class VertexTypes {
    GOAL, MARK, TASK, STAFF, WORKER
}

open class BaseVertex(val id: String) {
    override fun hashCode(): Int {
        return id.hashCode();
    }

    override fun equals(other: Any?) = when(other) {
        is BaseVertex -> {
            id == other.id
        }
        else -> false
    }
}

class GoalVertex(id: String): BaseVertex(id)

class MarkVertex(id: String): BaseVertex(id)

class TaskVertex(id: String, val taskId: String = "", val name: String = "", val points: Double = 1.0): BaseVertex(id);

class StaffVertex(id: String): BaseVertex(id)

class WorkerVertex(id: String, val dedication: Double = 1.0): BaseVertex(id)