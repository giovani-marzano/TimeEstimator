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
import java.lang.NumberFormatException
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.Comparator
import kotlin.math.floor
import kotlin.math.min
import kotlin.math.roundToInt
import kotlin.random.Random

// Program params
const val INPUT_GRAPHML = "./input.graphml"
const val INPUT_DISTRIBUTION = "./taskDistribution.txt"
const val NUM_SIMULATIONS = 100
const val OUTPUT_FILE = "./output.txt"

fun main(args: Array<String>) {
    val graphMlImporter = createGraphMLImporter()
    val graph = SimpleDirectedGraph<BaseVertex, DefaultEdge>(DefaultEdge::class.java)

    val inStream = File(INPUT_GRAPHML).inputStream()
    graphMlImporter.importGraph(graph, inStream)
    inStream.close()

    val tasksGraph = extractTasksSubGraph(graph)
    val workerSet = extractWorkers(graph)
    val distribution = importDistribution(INPUT_DISTRIBUTION)
    val random = Random.Default

    val workSimulator = WorkSimulator(
        taskDependencyGraph = tasksGraph,
        workerSet = workerSet,
        random = random,
        taskTimeDistribution = distribution
    )

    val writer = File(OUTPUT_FILE).writer()

    val marks = tasksGraph.vertexSet().asSequence()
        .map { it as? MarkVertex }
        .filterNotNull()
        .sortedBy { it.priority }
        .toList()

    writer.write(marks.joinToString(separator = ";") { "$it" })
    writer.write("\n")

    val begin = Instant.now()
    println("Beginig $NUM_SIMULATIONS simulations...")
    for (i in 1..NUM_SIMULATIONS) {
        workSimulator.simulateWork()
        writer.write(marks.joinToString(separator = ";") { "${workSimulator.finishTimes[it]}" })
        writer.write("\n")
        println("Simulation $i of $NUM_SIMULATIONS - ${Duration.between(begin, Instant.now())}")
    }
    println("...done ${Duration.between(begin, Instant.now())}")

    writer.close()
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

fun importDistribution(fileName: String): (Double) -> Double {
    val values = mutableListOf<Double>()
    var count = 0
    File(fileName).forEachLine { line ->
        try {
            values.add(line.toDouble())
            count++
        } catch (ex: NumberFormatException) {
            throw Exception("$fileName:$count ${ex.localizedMessage}", ex)
        }
    }

    val sortedValues = values.sorted()

    return fun(probValue: Double): Double {
        val idx = floor(probValue * sortedValues.size).roundToInt()
        return sortedValues[idx]
    }
}

fun extractWorkers(graph: Graph<BaseVertex, DefaultEdge>): Set<WorkerVertex> {
    return graph.vertexSet().asSequence()
        .map { it as? WorkerVertex }
        .filterNotNull()
        .toSet()
}

data class Work(val worker: WorkerVertex, val finishTime: Double, val task: TaskVertex? = null);

val finishTimeComparator = Comparator.comparing<Work, Double> { it.finishTime }

class WorkSimulator(
    val taskDependencyGraph: Graph<BaseVertex, DefaultEdge>,
    val workerSet: Set<WorkerVertex>,
    val taskTimeDistribution: (Double) -> Double,
    val random: Random = Random.Default
) {
    val finishTimes = mutableMapOf<BaseVertex, Double>()
    val todoQueue: Queue<TaskVertex> = PriorityQueue(taskPriorityComparator)
    val workQueue: Queue<Work> = PriorityQueue(finishTimeComparator)
    val idleQueue: Queue<WorkerVertex> = LinkedList()
    val statusMap = mutableMapOf<BaseTaskVertex, TaskStatus>()

    var time = 0.0

    private fun initialize() {
        time = 0.0
        finishTimes.clear()
        todoQueue.clear()
        workQueue.clear()
        idleQueue.clear()
        statusMap.clear()

        initStatusMap()
        initIdleQueue()
        initTodoQueue()
    }

    fun simulateWork() {
        initialize()

        if (idleQueue.isEmpty()) {
            throw Exception("No idle worker at the begin of simulation")
        }

        while (todoQueue.isNotEmpty() || workQueue.isNotEmpty()) {
            giveTasksToIdleWorkers()
            processFinishedWorkAdvancingTime()
        }
    }

    private fun giveTasksToIdleWorkers() {
        while (todoQueue.isNotEmpty() && idleQueue.isNotEmpty()) {
            val worker = idleQueue.remove()

            var task: TaskVertex? = null
            var workTime = taskTimeDistribution(random.nextDouble())

            if (random.nextDouble() < worker.dedication) {
                task = todoQueue.remove()
                workTime = task.points * workTime
            }

            workQueue.add(Work(worker = worker, task = task, finishTime = time + workTime))
        }
    }

    private fun processFinishedWorkAdvancingTime() {
        if (workQueue.isNotEmpty()) {
            val work = workQueue.remove()

            time = work.finishTime

            idleQueue.add(work.worker)

            work?.task?.also(this::processFinishedTask)
        }
    }

    private fun processFinishedTask(task: BaseTaskVertex) {
        statusMap[task] = TaskStatus.DONE
        finishTimes[task] = time
        enableDependentTasks(task)
    }

    private fun enableDependentTasks(task: BaseTaskVertex) {
        taskDependencyGraph.outgoingEdgesOf(task).asSequence()
            .map { taskDependencyGraph.getEdgeTarget(it) }
            .forEach { dependent ->
                when (dependent) {
                    is TaskVertex -> if (taskIsWorkable(dependent)) {
                        todoQueue.add(dependent)
                    }
                    is MarkVertex -> if (taskDependenciesAreDone(dependent)) {
                        processFinishedTask(dependent)
                    }
                }
            }
    }

    private fun initStatusMap() {
        TopologicalOrderIterator(taskDependencyGraph)
            .forEach {
                if (it is TaskVertex) {
                    statusMap[it] = if (it.status != TaskStatus.DONE) TaskStatus.PENDING else TaskStatus.DONE
                } else if (it is MarkVertex) {
                    statusMap[it] = if (taskDependenciesAreDone(it)) TaskStatus.DONE else TaskStatus.PENDING
                }

                if (statusMap[it] == TaskStatus.DONE) {
                    finishTimes[it] = time
                }
            }
    }

    private fun initIdleQueue() {
        idleQueue.addAll(workerSet)
    }

    private fun initTodoQueue() {
        for (task in statusMap.keys) {
            if (taskIsWorkable(task)) {
                if (task is TaskVertex) {
                    todoQueue.add(task)
                }
            }
        }
    }

    private fun taskIsWorkable(task: BaseVertex) =
        statusMap[task] == TaskStatus.PENDING && taskDependenciesAreDone(task)

    private fun taskDependenciesAreDone(task: BaseVertex): Boolean {
        return taskDependencyGraph.inDegreeOf(task) == 0 || taskDependencyGraph.incomingEdgesOf(task).asSequence()
            .map { taskDependencyGraph.getEdgeSource(it) }
            .all { statusMap[it] == TaskStatus.DONE }
    }
}
