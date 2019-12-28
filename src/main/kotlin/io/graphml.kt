package timeestimator.io

import org.jgrapht.graph.DefaultEdge
import org.jgrapht.io.*
import timeestimator.*
import java.lang.NumberFormatException

fun createGraphMLImporter(): GraphImporter<BaseVertex, DefaultEdge> {
    val vertexProviderFun = VertexProvider<BaseVertex>(::vertexProvider)
    val edgeProviderFun = EdgeProvider<BaseVertex, DefaultEdge>(::edgeProvider)

    val importer = GraphMLImporter(vertexProviderFun, edgeProviderFun)
    importer.isSchemaValidation = false;

    return importer
}

fun vertexProvider(id: String, attributes: Map<String, Attribute>): BaseVertex {
    return when (val type = extractVertexType(id, attributes)) {
        VertexTypes.MARK -> markVertexProvider(id, attributes)
        VertexTypes.TASK -> taskVertexProvider(id, attributes)
        VertexTypes.WORKER -> workerVertexProvider(id, attributes)
        VertexTypes.GOAL -> PrioritizedVertex(id, type)
        else -> BaseVertex(id, type)
    }
}

fun markVertexProvider(id: String, attributes: Map<String, Attribute>): MarkVertex {
    val taskId = attributes["taskId"]?.value ?: ""
    val name = attributes["name"]?.value ?: ""

    return MarkVertex(id, taskId, name)
}

fun taskVertexProvider(id: String, attributes: Map<String, Attribute>): TaskVertex {
    val taskId = attributes["taskId"]?.value ?: ""
    val name = attributes["name"]?.value ?: ""
    val pointsStr = attributes["points"]?.value ?: "1.0"
    val points = try {
        pointsStr.toDouble()
    } catch (e: NumberFormatException) {
        throw Exception("Vertex $id: invalid points value '$pointsStr'", e)
    }
    val status = extractTaskStatus(id, attributes)
    return TaskVertex(id, taskId, name, points, status)
}

fun workerVertexProvider(id: String, attributes: Map<String, Attribute>): WorkerVertex {
    val dedicationStr = attributes["dedication"]?.value ?: "1.0"
    val dedication = try {
        dedicationStr.toDouble()
    } catch (e: NumberFormatException) {
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

private fun extractTaskStatus(id: String, attributes: Map<String, Attribute>): TaskStatus {
    val statusStr = attributes["status"]?.value?.toUpperCase() ?: "NONE"
    return try {
        TaskStatus.valueOf(statusStr)
    } catch (ex: java.lang.IllegalArgumentException) {
        throw Exception("Vertex $id has invalid status '$statusStr'", ex)
    }
}

fun edgeProvider(from: BaseVertex, to: BaseVertex, label: String, attributes: Map<String, Attribute>) = DefaultEdge()
