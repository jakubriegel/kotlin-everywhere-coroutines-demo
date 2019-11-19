package eu.jrie.ke.demo

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.consumeEach
import kotlinx.coroutines.channels.produce
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File

class Demo

@ExperimentalCoroutinesApi
fun main(args: Array<String>) {
    when (args.first()) {
        "flow" -> flowDemo()
        "channel" -> channelDemo()
        else -> Unit
    }
}

data class User (
    val firstName: String,
    val lastName: String,
    val email: String,
    val score: Int
)

data class Score (
    val userId: String,
    val points: Int
)


@ExperimentalCoroutinesApi
fun flowDemo() = runBlocking {
    val data = File(Demo::class.java.getResource("/users.csv").file)
    var count = 0
    val n = data.linesFlow()
        .map { it.split(',') }
        .map { User(it[0], it[1], it[2], it[3].toInt()) }
        .map { it.score }
        .onEach { count++ }
        .fold(0) { i, score -> i + score }

    val average = n / count
    println("Average score is: $average")
}

private fun File.linesFlow() = flow {
    bufferedReader()
        .lineSequence()
        .forEach { emit(it) }
}

@ExperimentalCoroutinesApi
fun channelDemo() = runBlocking {
    val data = File(Demo::class.java.getResource("/scores.csv").file)
    val db = mutableMapOf<String, Int>()

    val start = System.nanoTime()

    val dataChannel = produce(capacity = 1_000) {
        data.bufferedReader()
            .lineSequence()
            .forEach { send(it) }
    }
    val scoreChannel = produce(capacity = 1_000) {
        dataChannel.consumeEach { line ->
            line.split(',')
                .let { Score(it[0], it[1].toInt()) }
                .also { send(it) }
        }
    }

    launch {
        scoreChannel.consumeEach {
//            delay(1)
            db.compute(it.userId) { _, v ->
                (v ?: 0) + it.points
            }
        }
    } .join()

//    List(1_000) {
//        launch {
//            scoreChannel.consumeEach {
//                delay(1)
//                db.compute(it.userId) { _, v ->
//                    (v ?: 0) + it.points
//                }
//            }
//        }
//    } .forEach { it.join() }

    val end = System.nanoTime()

    db.toList()
        .sortedByDescending { (_, v) -> v }
        .take(5)
        .forEach { (id, score) -> println("$id -> $score") }

    println("\nexecution time: ${end - start}")

}
