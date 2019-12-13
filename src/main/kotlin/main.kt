package timeestimator

import org.jgrapht.Graph
import org.jgrapht.event.ConnectedComponentTraversalEvent
import org.jgrapht.event.EdgeTraversalEvent
import org.jgrapht.event.TraversalListener
import org.jgrapht.event.VertexTraversalEvent
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.EdgeReversedGraph
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.DepthFirstIterator
import org.jgrapht.traverse.TopologicalOrderIterator
import timeestimator.io.createGraphMLImporter
import java.io.File
import kotlin.math.min

// Program params
const val INPUT_GRAPHML = "./input.graphml"

fun main(args: Array<String>) {
    val graphMlImporter = createGraphMLImporter()
    val graph = SimpleDirectedGraph<BaseVertex, DefaultEdge>(DefaultEdge::class.java)

    val inStream = File(INPUT_GRAPHML).inputStream()
    graphMlImporter.importGraph(graph, inStream)
    inStream.close()

    val tasksGraph = extractTasksSubGraph(graph)

    TopologicalOrderIterator(tasksGraph, Comparator { t1, t2 ->
        val p1 = (t1 as? BaseTaskVertex)?.let { it.priority } ?: 0
        val p2 = (t2 as? BaseTaskVertex)?.let { it.priority } ?: 0
        p1 - p2
    }).asSequence()
        .forEach { println(it) }
}

fun extractTasksSubGraph(graph: Graph<BaseVertex, DefaultEdge>): Graph<BaseVertex, DefaultEdge> {

    val goalVertices = graph.vertexSet().asSequence()
        .filter { it.type == VertexTypes.GOAL }
        .toSet()

    val subGraph = SimpleDirectedGraph<BaseVertex, DefaultEdge>(DefaultEdge::class.java)

    val addVertexToSubGraph = fun(vertex: BaseVertex) {
        subGraph.addVertex(vertex)
        graph.incomingEdgesOf(vertex).forEach { edge ->
            val source = graph.getEdgeSource(edge)
            subGraph.addVertex(source)
            subGraph.addEdge(source, vertex, edge)
        }
    }

    val allowedTypes = setOf(VertexTypes.GOAL, VertexTypes.MARK, VertexTypes.TASK)

    val reversedGraph = EdgeReversedGraph(graph)
    val depthFirstIterator = DepthFirstIterator(reversedGraph, goalVertices)

    depthFirstIterator.addTraversalListener(TaskPriorityTraversalListener(reversedGraph))

    depthFirstIterator.asSequence()
        .filter { allowedTypes.contains(it.type) }
        .forEach { addVertexToSubGraph(it) }

    graph.vertexSet().asSequence()
        .filter { it.type == VertexTypes.MARK }
        .filterNot { subGraph.containsVertex(it) }
        .filter { mark ->
            graph.incomingEdgesOf(mark).asSequence()
                .map { graph.getEdgeSource(it) }
                .all { subGraph.containsVertex(it) }
        }.forEach { addVertexToSubGraph(it) }

    return subGraph
}

class TaskPriorityTraversalListener(val graph: Graph<BaseVertex, DefaultEdge>, val markPriorityDelta: Int = 100) :
    TraversalListener<BaseVertex, DefaultEdge> {
    private var priority = 0

    override fun connectedComponentStarted(e: ConnectedComponentTraversalEvent?) {
        priority = 0
    }

    override fun connectedComponentFinished(e: ConnectedComponentTraversalEvent?) {
    }

    override fun vertexTraversed(e: VertexTraversalEvent<BaseVertex>?) {
        val vertex = e?.vertex ?: return

        if (vertex is MarkVertex) {
            priority -= markPriorityDelta
            vertex.priority = priority
        }
    }

    override fun vertexFinished(e: VertexTraversalEvent<BaseVertex>?) {
        val vertex = e?.vertex ?: return
        if (vertex is MarkVertex) {
            priority += markPriorityDelta
        }
    }

    override fun edgeTraversed(e: EdgeTraversalEvent<DefaultEdge>?) {
        val edge = e?.edge ?: return
        val vertex = graph.getEdgeTarget(edge)

        if (vertex is TaskVertex) {
            val newPriority = priority - graph.inDegreeOf(vertex)
            vertex.priority = min(vertex.priority, newPriority)
        }
    }
}
