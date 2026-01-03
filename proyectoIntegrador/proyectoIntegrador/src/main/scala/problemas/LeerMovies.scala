package problemas

import cats.effect.{IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

case class Movie(
                  adult: Boolean,
                  budget: Double,
                  revenue: Double,
                  popularity: Double,
                  runtime: Double,
                  vote_average: Double,
                  vote_count: Double,
                  original_language: String,
                  status: String
                )


given CsvRowDecoder[Movie, String] = deriveCsvRowDecoder[Movie]

object LeerMovies extends IOApp.Simple:

  val run: IO[Unit] =

    val filePath = Path("src/main/resources/data/pi-movies-complete-2025-12-04.csv")

    Files[IO]
    .readAll(filePath)
    .through(text.utf8.decode)
    .through(decodeUsingHeaders[Movie](';'))
    .compile
    .toList
    .flatMap { movies =>
      //Leer columnas numericas
        val budgets       = movies.map(_.budget)
        val revenues      = movies.map(_.revenue)
        val popularities  = movies.map(_.popularity)
        val runtimes      = movies.map(_.runtime)
        val voteAverages  = movies.map(_.vote_average)
        val voteCounts    = movies.map(_.vote_count)

      //Analisis de Datos (Estadisticas Basicas)
        def promedio(xs: List[Double]) =
          if xs.isEmpty then 0.0 else xs.sum / xs.length

        val avgRevenue = promedio(revenues)
        val avgRuntime = promedio(runtimes)
        val avgVote    = promedio(voteAverages)

        //Analisis de Datos (Tipo Texto)
        val frecuenciaIdioma =
          movies.groupBy(_.original_language).mapValues(_.size)

        val frecuenciaAdult =
          movies.groupBy(_.adult).mapValues(_.size)

        //Limpieza de Datos
        val moviesLimpias =
          movies.filter(m =>
            m.budget >= 0 &&
              m.revenue >= 0 &&
              m.popularity >= 0 &&
              m.runtime > 0 &&
              m.vote_average >= 0 &&
              m.vote_average <= 10 &&
              m.vote_count >= 0
          )

        IO.println("ANALISIS DATASET MOVIES") >>
          IO.println(s"Total registros originales: ${movies.length}") >>
          IO.println(s"Total registros limpios:    ${moviesLimpias.length}") >>
          IO.println("") >>
          IO.println("PROMEDIOS (COLUMNAS NUMERICAS)") >>
          IO.println(f"Revenue promedio:   $avgRevenue%.2f") >>
          IO.println(f"Runtime promedio:   $avgRuntime%.2f") >>
          IO.println(f"Vote average prom.: $avgVote%.2f") >>
          IO.println("") >>
          IO.println("FRECUENCIA POR IDIOMA") >>
          frecuenciaIdioma.toList
            .sortBy(-_._2)
            .take(5)
            .map { case (lang, count) =>
              IO.println(s"$lang -> $count")
            }
            .foldLeft(IO.unit)(_ >> _) >>
          IO.println("") >>
          IO.println("FRECUENCIA ADULT") >>
          frecuenciaAdult.toList
            .map { case (adult, count) =>
              IO.println(s"$adult -> $count")
            }
            .foldLeft(IO.unit)(_ >> _)
      }
