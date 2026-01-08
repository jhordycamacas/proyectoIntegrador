package problemas

package problemas

import cats.effect.{IO, IOApp}
import cats.syntax.all.*
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*

import models.Movie


// DECODERS CSV

given CellDecoder[Double] =
CellDecoder.stringDecoder.map { s =>
  s.trim.toDoubleOption.getOrElse(0.0)
}

given CellDecoder[Boolean] =
  CellDecoder.stringDecoder.map { s =>
    s.trim.toLowerCase match
      case "true"  => true
      case "false" => false
      case _       => false
  }

given CellDecoder[String] =
  CellDecoder.stringDecoder.map(_.trim)

// Decoder de fila (usa los headers del CSV)
given CsvRowDecoder[Movie, String] =
deriveCsvRowDecoder[Movie]


// NORMALIZADORES

def normalizarDouble(v: Option[Double]): Double =
  v match
    case Some(x) if x >= 0 => x
    case _ => 0.0

def normalizarBoolean(v: Option[Boolean]): Boolean =
  v.getOrElse(false)

def normalizarString(v: Option[String]): String =
  v match
    case Some(s) if s.nonEmpty => s
    case _ => "UNKNOWN"


// MODELO LIMPIO

case class MovieClean(
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

def limpiarMovies(movies: List[Movie]): List[MovieClean] =
  movies.map { m =>
    MovieClean(
      adult = normalizarBoolean(m.adult),
      budget = normalizarDouble(m.budget),
      revenue = normalizarDouble(m.revenue),
      popularity = normalizarDouble(m.popularity),
      runtime = normalizarDouble(m.runtime),
      vote_average = normalizarDouble(m.vote_average),
      vote_count = normalizarDouble(m.vote_count),
      original_language = normalizarString(m.original_language),
      status = normalizarString(m.status)
    )
  }

def resumenDouble(nombre: String, valores: List[Option[Double]]): IO[Unit] = {
  val total = valores.size
  val validos = valores.count(v => v.exists(_ >= 0))
  val invalidos = total - validos

  IO.println(s"\n$nombre") >>
    IO.println(s"  Total: $total") >>
    IO.println(s"  Válidos: $validos") >>
    IO.println(s"  Inválidos-corregidos: $invalidos") >>
    IO.println(f"  Validos: ${validos * 100.0 / total}%.2f%%")
}

def resumenBoolean(nombre: String, valores: List[Option[Boolean]]): IO[Unit] = {
  val total = valores.size
  val validos = valores.count(_.isDefined)
  val invalidos = total - validos

  IO.println(s"\n$nombre") >>
    IO.println(s"  Total: $total") >>
    IO.println(s"  Válidos: $validos") >>
    IO.println(s"  Inválidos: $invalidos")
}

def resumenString(nombre: String, valores: List[Option[String]]): IO[Unit] = {
  val total = valores.size
  val validos = valores.count(v => v.exists(_.nonEmpty))
  val invalidos = total - validos

  IO.println(s"\n$nombre") >>
    IO.println(s"  Total: $total") >>
    IO.println(s"  Válidos: $validos") >>
    IO.println(s"  Vacíos-null: $invalidos")
}



// PROGRAMA PRINCIPAL

object Avance2LimpiezaDatos extends IOApp.Simple:

  val filePath =
    Path("src/main/resources/data/pi-movies-complete-2025-12-04.csv")

  val run: IO[Unit] =
    Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[Movie](';'))
      .compile
      .toList
      .flatMap { movies =>

        for {
          _ <- IO.println(s"Total de registros leídos: ${movies.size}")

          _ <- resumenBoolean("Adult", movies.map(_.adult))
          _ <- resumenDouble("Budget", movies.map(_.budget))
          _ <- resumenDouble("Revenue", movies.map(_.revenue))
          _ <- resumenDouble("Popularity", movies.map(_.popularity))
          _ <- resumenDouble("Runtime", movies.map(_.runtime))
          _ <- resumenDouble("Vote Average", movies.map(_.vote_average))
          _ <- resumenDouble("Vote Count", movies.map(_.vote_count))
          _ <- resumenString("Original Language", movies.map(_.original_language))
          _ <- resumenString("Status", movies.map(_.status))

          // ==============================
          // LIMPIEZA FINAL
          // ==============================
          moviesClean = limpiarMovies(movies)

          _ <- IO.println(s"\nTotal de películas procesadas: ${moviesClean.size}")

          _ <- IO.println("\nIDIOMAS MÁS COMUNES (TOP 10):")
          _ <- moviesClean
            .groupBy(_.original_language)
            .view.mapValues(_.size)
            .toSeq
            .sortBy(-_._2)
            .take(10)
            .traverse_ { case (lang, count) =>
              IO.println(f"  $lang%-10s -> $count%,6d")
            }

          _ <- IO.println("\nESTADOS MÁS COMUNES (TOP 10):")
          _ <- moviesClean
            .groupBy(_.status)
            .view.mapValues(_.size)
            .toSeq
            .sortBy(-_._2)
            .take(10)
            .traverse_ { case (status, count) =>
              IO.println(f"  $status%-20s -> $count%,6d")
            }

        } yield ()
      }



