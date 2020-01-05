package timeestimator

import org.jgrapht.Graph
import org.jgrapht.event.*
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.EdgeReversedGraph
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.TopologicalOrderIterator
import timeestimator.io.createGraphMLImporter
import java.io.File
import java.io.Writer
import java.lang.NumberFormatException
import java.time.Duration
import java.time.Instant
import java.util.*
import kotlin.Comparator
import kotlin.math.floor
import kotlin.math.roundToInt
import kotlin.math.sqrt
import kotlin.random.Random

// Program params
const val INPUT_GRAPHML = "./input.graphml"
const val INPUT_DISTRIBUTION = "./taskDistribution.txt"
const val NUM_SIMULATIONS = 16000
const val OUTPUT_FINISH_TIMES = "./finishTimes.txt"
const val OUTPUT_PRIORITIES = "./priorities.txt"
const val OUTPUT_STATS = "./statistics.txt"
const val OUTPUT_FIELD_SEPARATOR = "\t"
val QUANTILES = listOf(0.05,0.25,0.75,0.95)

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

    File(OUTPUT_PRIORITIES).writer().use { writer ->
        TopologicalOrderIterator(tasksGraph, taskPriorityComparator)
            .forEach { task ->
                writer.write("$task\n")
            }
    }

    val workSimulator = WorkSimulator(
        taskDependencyGraph = tasksGraph,
        workerSet = workerSet,
        random = random,
        taskTimeDistribution = distribution
    )

    val marks = tasksGraph.vertexSet().asSequence()
        .map { it as? MarkVertex }
        .filterNotNull()
        .sortedBy { it.priority }
        .toList()

    val statsMap = marks.map { it to MarkStatistics(NUM_SIMULATIONS, QUANTILES) }.toMap()

    File(OUTPUT_FINISH_TIMES).writer().use { writer ->
        writer.write(marks.joinToString(separator = OUTPUT_FIELD_SEPARATOR) { taskHeader(it) })
        writer.write("\n")

        val begin = Instant.now()
        println("Beginig $NUM_SIMULATIONS simulations...")
        for (i in 1..NUM_SIMULATIONS) {
            workSimulator.simulateWork()

            marks.forEach { statsMap[it]?.addNullableSample(workSimulator.finishTimes[it]) }

            writer.write(marks.joinToString(separator = OUTPUT_FIELD_SEPARATOR) { "${workSimulator.finishTimes[it]}" })
            writer.write("\n")
            println("Simulation $i of $NUM_SIMULATIONS - ${Duration.between(begin, Instant.now())}")
        }
        println("...done ${Duration.between(begin, Instant.now())}")
    }

    writeStatistics(marks.sortedBy { statsMap[it]?.allSamplesMeanVar?.average }, statsMap, workerSet)
}

private fun writeStatistics(
    marks: List<MarkVertex>,
    statsMap: Map<MarkVertex, MarkStatistics>,
    workerSet: Set<WorkerVertex>
) {
    File(OUTPUT_STATS).writer().use { writeStatisticsReport(it, marks, statsMap, workerSet) }
}

fun writeStatisticsReport(
    writer: Writer,
    marks: List<MarkVertex>,
    statsMap: Map<MarkVertex, MarkStatistics>,
    workerSet: Set<WorkerVertex>
): Unit {

    writer.write("\n##############################################")
    writer.write("\n# Report ${Instant.now()}")
    writer.write("\n##############################################\n\n")


    writer.write("Workers: " + workerSet.joinToString(separator = ", ") { "${it.dedication}" })
    writer.write("\n")
    writer.write("Num. simulations: ${NUM_SIMULATIONS}\n")

    marks.forEach { writeMarkStatistics(writer, it, statsMap[it] ) }
}

fun numberFmt(x: Double?) = "%.2f".format(Locale.ROOT, x)

private fun writeMarkStatistics(writer: Writer, mark: MarkVertex, stats: MarkStatistics?) : Unit {

    writer.write("\n")
    writer.write(taskHeader(mark) + "\n")

    if (stats != null) {
        writeAverageWithConfidenceInterval99_7(writer, "average", stats.allSamplesMeanVar)

        val stdevSample = stats.allSamplesMeanVar.stdevSample
        writer.write("- stdev sample: ${numberFmt(stdevSample)}\n")

        for ((index, quantStat) in stats.quantilesMeanVarList.withIndex()) {
            val quantStr = "%4.1f%%".format(stats.quantiles[index] * 100.0)
            writeAverageWithConfidenceInterval99_7(writer, quantStr, quantStat)
        }
    } else {
        writer.write("- NO STATS !!\n")
    }
}

fun writeAverageWithConfidenceInterval99_7(writer: Writer, statName: String, stats: MeanVarianceAccumulator) {
    val (minimun, maximun) = stats.confidenceInterval99_7
    val average = stats.average
    writer.write("- ${statName} (99.7%): ${numberFmt(average)} (${numberFmt(minimun)}; ${numberFmt(maximun)})\n")
}

fun taskHeader(task: BaseTaskVertex) = "${task.id}:${task.taskId}:${task.name}"

class MeanVarianceAccumulator {
    var n: Int = 0
        private set
    private var shift = 0.0
    private var accum = 0.0
    private var accum2 = 0.0

    fun addSample(x: Double) {
        if (n == 0) {
            shift = x
        }
        n++
        accum += (x - shift)
        accum2 += (x - shift) * (x - shift)
    }

    fun addNullableSample(x: Double?) {
        if (x != null) addSample(x)
    }

    val average: Double
        get() = accum / n + shift

    val variancePopulation: Double
        get() = (accum2 - (accum * accum) / n) / n

    val varianceSample: Double
        get() = variancePopulation * n / (n - 1)

    val stdevPopulation get() = sqrt(variancePopulation)

    val stdevSample get() = sqrt(varianceSample)

    val confidenceInterval99_7: Pair<Double, Double> get() {
        val confInterval = stdevSample / sqrt(1.0 * n) * 3
        val minimun = average - confInterval
        val maximun = average + confInterval
        return Pair(minimun, maximun)
    }
}

class MarkStatistics(numTotalSamples: Int, quantiles: List<Double>) {
    val allSamplesMeanVar = MeanVarianceAccumulator()
    val quantiles: List<Double>
    val quantilesMeanVarList: List<MeanVarianceAccumulator>
    val percentileSubSample = PriorityQueue<Double>()
    val subSampleSize: Int

    init {
        subSampleSize = floor(sqrt(numTotalSamples.toDouble())).roundToInt()
        this.quantiles = quantiles.sorted()
        quantilesMeanVarList = this.quantiles.map { MeanVarianceAccumulator() }
    }

    fun addNullableSample(x: Double?) {
        if (x != null) addSample(x)
    }

    fun addSample(x: Double) : Unit {
        allSamplesMeanVar.addSample(x)
        percentileSubSample.add(x)
        extractQuantiles()
    }

    private fun extractQuantiles(): Unit {
        if (percentileSubSample.size < subSampleSize) {
            return
        }

        var currentPos = 0
        var currentValue: Double? = percentileSubSample.poll()
        for ((index, quantile) in quantiles.withIndex()) {
            val targetPos = floor(quantile * subSampleSize).roundToInt() - 1
            while (targetPos > currentPos && currentPos < subSampleSize) {
                currentValue = percentileSubSample.poll()
                currentPos++
            }
            quantilesMeanVarList[index].addNullableSample(currentValue)
        }
        percentileSubSample.clear()
    }
}

fun extractTasksSubGraph(graph: Graph<BaseVertex, DefaultEdge>): Graph<BaseTaskVertex, DefaultEdge> {

    val subGraph = SimpleDirectedGraph<BaseTaskVertex, DefaultEdge>(DefaultEdge::class.java)

    val addVertexToSubGraph = fun(vertex: BaseTaskVertex) {
        subGraph.addVertex(vertex)
        graph.incomingEdgesOf(vertex).forEach { edge ->
            val source = graph.getEdgeSource(edge)
            if (source is BaseTaskVertex) {
                subGraph.addVertex(source)
                subGraph.addEdge(source, vertex, edge)
            }
        }
    }

    val reversedGraph = EdgeReversedGraph(graph)

    val prioritizeIterator = TopologicalOrderIterator(reversedGraph)

    prioritizeIterator.addTraversalListener(TaskPriorityTraversalListener(reversedGraph))

    prioritizeIterator.asSequence()
        .map { it as? BaseTaskVertex }
        .filterNotNull()
        .forEach { addVertexToSubGraph(it) }

    return subGraph
}

class TaskPriorityTraversalListener(
    private val graph: Graph<BaseVertex, DefaultEdge>,
    private val goalPriorityDelta: Int = 100
) : TraversalListenerAdapter<BaseVertex, DefaultEdge>() {
    override fun vertexFinished(event: VertexTraversalEvent<BaseVertex>?) {
        val vertex = event?.vertex

        if ( vertex !is PrioritizedVertex ) return

        val priority = graph.incomingEdgesOf(vertex).asSequence()
            .map { graph.getEdgeSource(it) }
            .map { it as? PrioritizedVertex }
            .filterNotNull()
            .map { it.priority }
            .min() ?: 0

        vertex.priority = priority

        if (vertex.type == VertexTypes.GOAL) vertex.priority -= goalPriorityDelta
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

data class Work(val worker: WorkerVertex, val finishTime: Double, val task: TaskVertex? = null)

val finishTimeComparator: Comparator<Work> = Comparator.comparing<Work, Double> { it.finishTime }

class WorkSimulator(
    val taskDependencyGraph: Graph<BaseTaskVertex, DefaultEdge>,
    val workerSet: Set<WorkerVertex>,
    val taskTimeDistribution: (Double) -> Double,
    val random: Random = Random.Default
) {
    val finishTimes = mutableMapOf<BaseTaskVertex, Double>()
    private val todoQueue: Queue<TaskVertex> = PriorityQueue(taskPriorityComparator)
    private val workQueue: Queue<Work> = PriorityQueue(finishTimeComparator)
    private val idleQueue: Queue<WorkerVertex> = LinkedList()
    private val statusMap = mutableMapOf<BaseTaskVertex, TaskStatus>()

    private var time = 0.0

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
                workTime *= task.points
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

    private fun taskIsWorkable(task: BaseTaskVertex) =
        statusMap[task] == TaskStatus.PENDING && taskDependenciesAreDone(task)

    private fun taskDependenciesAreDone(task: BaseTaskVertex): Boolean {
        return taskDependencyGraph.inDegreeOf(task) == 0 || taskDependencyGraph.incomingEdgesOf(task).asSequence()
            .map { taskDependencyGraph.getEdgeSource(it) }
            .all { statusMap[it] == TaskStatus.DONE }
    }
}
