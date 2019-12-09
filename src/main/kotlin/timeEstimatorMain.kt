package marzano.giovani.timeestimator

import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.io.*
import java.io.File
import java.lang.NumberFormatException
import kotlin.Exception

// Program params
const val INPUT_GRAPHML = "./input.graphml"

fun main(args: Array<String>) {
    val graphMlImporter =  createGraphMLImporter()
    val graph = SimpleDirectedGraph<BaseVertex, DefaultEdge>(DefaultEdge::class.java)

    val inStream = File(INPUT_GRAPHML).inputStream()
    graphMlImporter.importGraph(graph, inStream)
    inStream.close()
}

fun createGraphMLImporter(): GraphImporter<BaseVertex, DefaultEdge> {
    val vertexProviderFun = VertexProvider<BaseVertex>(::vertexProvider)
    val edgeProviderFun = EdgeProvider<BaseVertex, DefaultEdge>(::edgeProvider)

    val importer = GraphMLImporter(vertexProviderFun, edgeProviderFun)
    importer.isSchemaValidation = false;

    return importer
}

fun vertexProvider(id: String, attributes: Map<String, Attribute>): BaseVertex {
    return when (extractVertexType(id, attributes)) {
        VertexTypes.GOAL -> GoalVertex(id)
        VertexTypes.MARK -> MarkVertex(id)
        VertexTypes.TASK -> taskVertexProvider(id, attributes)
        VertexTypes.STAFF -> StaffVertex(id)
        VertexTypes.WORKER -> TODO()
    }
}

fun taskVertexProvider(id: String, attributes: Map<String, Attribute>): TaskVertex {
    val taskId = attributes["taskId"]?.value ?: ""
    val name = attributes["name"]?.value ?: ""
    val pointsStr = attributes["points"]?.value ?: "1.0"
    val points = try { pointsStr.toDouble() } catch(e: NumberFormatException) {
        throw Exception("Vertex $id: invalid points value '$pointsStr'", e)
    }
    return TaskVertex(id, taskId, name, points)
}

fun workerVertexProvider(id: String, attributes: Map<String, Attribute>): WorkerVertex {
    val dedicationStr = attributes["dedication"]?.value ?: "1.0"
    val dedication = try { dedicationStr.toDouble() } catch (e: NumberFormatException) {
        throw Exception("Vertex $id: invalid dedication value '$dedicationStr'")
    }
    if (dedication !in 0.0..1.0) {
        throw Exception("Vertex $id: dedication must be in range [0.0..1.0]")
    }

    return WorkerVertex(id, dedication)
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

fun edgeProvider(from: BaseVertex , to: BaseVertex, label: String, attributes: Map<String, Attribute>) = DefaultEdge()

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