package problemas

import cats.effect.{IO, IOApp}
import fs2.Stream
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import io.circe.*
import io.circe.parser.*
import io.circe.generic.auto.*
import io.circe.Decoder
import java.time.LocalDate
import scala.util.Try
import cats.implicits.*


case class BelongsToCollection (
                                 id: Int,
                                 name: String,
                                 poster_path: Option[String], // Puede ser nulo
                                 backdrop_path: Option[String] // Puede ser nulo
                                 )
case class Genre (
                  id: Int,
                  name: String
                  )
case class ProductionCompany (
                                name: String,
                                id: Int
                                )
case class ProductionCountry (
                                  iso_3166_1: String,
                                  name: String
                                )
case class SpokenLanguage (
                              iso_639_1: String,
                              name: String
                            )
case class Keyword (
                    id: Int,
                    name: String
                    )
case class Cast (
                cast_id: Int,
                character: String,
                credit_id: String,
                gender: Int,
                id: Int,
                name: String,
                order: Int,
                profile_path: Option[String] // Puede ser nulo
                )
case class Crew (
                credit_id: String,
                department: String,
                gender: Int,
                id: Int,
                job: String,
                name: String,
                profile_path: Option[String] // Puede ser nulo
                )
case class Rating (
                   userId: Int,
                   rating: Double,
                   timestamp: Long //Long por que el valor es grande
                   )
case class MovieRaw (
                    adult: Boolean,
                    belongs_to_collection: String,
                    budget: String,
                    genres: String,
                    homepage: String,
                    id: String,
                    imdb_id: String,
                    original_language: String,
                    original_title: String,
                    overview: String,
                    popularity: String,
                    poster_path: String,
                    production_companies: String,
                    production_countries: String,
                    release_date: String,
                    revenue: String,
                    runtime: String,
                    spoken_languages: String,
                    status: String,
                    tagline: String,
                    title: String,
                    video: Boolean,
                    vote_average: String,
                    vote_count: String,
                    keywords: String,
                    cast: String,
                    crew: String,
                    ratings: String
                    )
given CsvRowDecoder[MovieRaw, String] = deriveCsvRowDecoder[MovieRaw]


case class MovieClean(
                       adult: Boolean,
                       belongs_to_collection: Option[BelongsToCollection],
                       budget: Option[Long],
                       genres: List[Genre],
                       homepage: Option[String],
                       id: Option[Int],
                       imdb_id: Option[String],
                       original_language: Option[String],
                       original_title: Option[String],
                       overview: Option[String],
                       popularity: Option[Double],
                       poster_path: Option[String],
                       production_companies: List[ProductionCompany],
                       production_countries: List[ProductionCountry],
                       release_date: Option[String],
                       revenue: Option[Long],
                       runtime: Option[Int],
                       spoken_languages: List[SpokenLanguage],
                       status: Option[String],
                       tagline: Option[String],
                       title: Option[String],
                       video: Boolean,
                       vote_average: Option[Double],
                       vote_count: Option[Int],
                       keywords: List[Keyword],
                       cast: List[Cast],
                       crew: List[Crew],
                       ratings: List[Rating]
                     )
object CirceProcesos {

  private def fixJson(raw: String): String = {
    if (raw == null || raw == "None" || raw.isEmpty) "null"
    else {
      raw.replace("None", "null")
        .replaceAll("(?<=[\\s\\[\\{\\:,])'|'(?=[\\s\\]\\}\\,:])", "\"")
        .trim
    }
  }

  private def isArray(raw: String): Boolean =
    raw.startsWith("[")

  private def isObject(raw: String): Boolean =
    raw.startsWith("{")

  def parseSafe[T: Decoder](json: String): Either[String, Either[List[T], T]] = {
    Option(json).map(_.trim).filter(s => s.nonEmpty && s != "null" && s != "None") match {
      case Some(validJson) =>
        val fixed = fixJson(validJson)
        if (isArray(fixed)) {
          parser.parse(fixed).flatMap(_.as[List[T]]) match {
            case Right(lst) => Right(Left(lst))
            case Left(err) =>
              Left(s"Error parsing JSON list: $err, JSON: $fixed")
          }
        } else if (isObject(fixed)) {
          parser.parse(fixed).flatMap(_.as[T]) match {
            case Right(obj) => Right(Right(obj))
            case Left(err) =>
              Left(s"Error parsing JSON object: $err, JSON: $fixed")
          }
        } else {
          Left(s"JSON no reconocido: $fixed")
        }

      case None => Left("JSON vacío o nulo")
    }
  }

  /** Helpers para usar directamente como antes */
  def parseList[T: Decoder](json: String): List[T] =
    parseSafe[T](json) match {
      case Right(Left(lst)) => lst
      case _                => Nil
    }

  def parseOption[T: Decoder](json: String): Option[T] =
    parseSafe[T](json) match {
      case Right(Right(obj)) => Some(obj)
      case _                 => None
    }
}


def toIntOption(s: String): Option[Int] =
  s.trim.toIntOption

def toLongOption(s: String): Option[Long] =
  s.trim.toLongOption

def toDoubleOption(s: String): Option[Double] =
  s.trim.toDoubleOption

def toMovieClean(raw: MovieRaw): MovieClean =
  MovieClean(
    adult = raw.adult,
    belongs_to_collection = CirceProcesos.parseOption[BelongsToCollection](raw.belongs_to_collection),
    budget = toLongOption(raw.budget),
    genres = CirceProcesos.parseList[Genre](raw.genres),
    homepage = cleanTextOpt(Some(raw.homepage)),
    id = toIntOption(raw.id),
    imdb_id = cleanTextOpt(Some(raw.imdb_id)),
    original_language = cleanTextOpt(Some(raw.original_language)),
    original_title = cleanTextOpt(Some(raw.original_title)),
    overview = cleanTextOpt(Some(raw.overview)),
    popularity = toDoubleOption(raw.popularity),
    poster_path = cleanTextOpt(Some(raw.poster_path)),
    production_companies = CirceProcesos.parseList[ProductionCompany](raw.production_companies),
    production_countries = CirceProcesos.parseList[ProductionCountry](raw.production_countries),
    release_date = parseDateOpt(cleanTextOpt(Option(raw.release_date))),
      revenue = toLongOption(raw.revenue),
    runtime = toIntOption(raw.runtime),
    spoken_languages = CirceProcesos.parseList[SpokenLanguage](raw.spoken_languages),
    status = cleanTextOpt(Some(raw.status)),
    tagline = cleanTextOpt(Some(raw.tagline)),
    title = cleanTextOpt(Some(raw.title)),
    video = raw.video,
    vote_average = toDoubleOption(raw.vote_average),
    vote_count = toIntOption(raw.vote_count),
    keywords = CirceProcesos.parseList[Keyword](raw.keywords),
    cast = CirceProcesos.parseList[Cast](raw.cast),
    crew = CirceProcesos.parseList[Crew](raw.crew),
    ratings = CirceProcesos.parseList[Rating](raw.ratings)
  )


def cleanTextOpt(s: Option[String]): Option[String] =
  s.map(_.trim).filter(_.nonEmpty)

def parseDateOpt(s: Option[String]): Option[String] = s.flatMap(str => Try(LocalDate.parse(str.trim)).toOption).map(_.toString)

def cleanMovie(m: MovieClean): MovieClean = {
  def positiveLongOpt(n: Option[Long]): Option[Long] = n.filter(_ > 0)
  def positiveIntOpt(n: Option[Int]): Option[Int] = n.filter(_ > 0)
  def validRatingOpt(r: Rating): Option[Rating] = if (r.rating >= 0 && r.rating <= 5) Some(r) else None
  def cleanRatings(rs: List[Rating]): List[Rating] = rs.flatMap(validRatingOpt)
  def cleanCast(cs: List[Cast]): List[Cast] = cs.filter(c => c.name.nonEmpty && c.character.nonEmpty)

  m.copy(
    title = cleanTextOpt(m.title),
    overview = cleanTextOpt(m.overview),
    release_date = parseDateOpt(m.release_date),
    budget = positiveLongOpt(m.budget),
    revenue = positiveLongOpt(m.revenue),
    runtime = positiveIntOpt(m.runtime),
    popularity = m.popularity.filter(_ >= 0),
    vote_average = m.vote_average.filter(r => r >= 0 && r <= 5),
    vote_count = positiveIntOpt(m.vote_count),
    ratings = cleanRatings(m.ratings),
    cast = cleanCast(m.cast),
    genres = m.genres.map(g => g.copy(name = g.name.trim)).filter(_.name.nonEmpty),
    production_companies = m.production_companies.map(pc => pc.copy(name = pc.name.trim)).filter(_.name.nonEmpty),
    production_countries = m.production_countries.map(pc => pc.copy(name = pc.name.trim)).filter(_.name.nonEmpty),
    spoken_languages = m.spoken_languages.map(sl => sl.copy(name = sl.name.trim)).filter(_.name.nonEmpty)
  )
}


object Circe extends IOApp.Simple {

  val filePath = Path("src/main/resources/data/pi-movies-complete-2025-12-04.csv")

  val stream: Stream[IO, MovieClean] =
    Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieRaw](';'))
      .map(toMovieClean)
      .map(cleanMovie)

  val run: IO[Unit] = for {
    movies <- stream.compile.toList


    totalCleaned = movies.size
    moviesConGeneros = movies.count(_.genres.nonEmpty)
    moviesConCast    = movies.count(_.cast.nonEmpty)
    moviesConId      = movies.count(_.id.isDefined)


    totalOriginal <- Files[IO]
      .readAll(filePath)
      .through(text.utf8.decode)
      .through(decodeUsingHeaders[MovieRaw](';'))
      .compile
      .toList
      .map(_.size)

    _ <- IO.println("=" * 50)
    _ <- IO.println(s"Total registros originales: $totalOriginal")
    _ <- IO.println(s"Total registros procesados: $totalCleaned")


    _ <- IO.println(s"Películas con géneros recuperados: $moviesConGeneros")
    _ <- IO.println(s"Películas con cast recuperado:    $moviesConCast")
    _ <- IO.println(s"Películas con ID válido:          $moviesConId")


    _ <- IO.println(f"Porcentaje conservado:      ${totalCleaned.toDouble / totalOriginal * 100}%.2f%%")
    _ <- IO.println("=" * 50)
    _ <- IO.println("Ejemplo de 5 películas limpias:")
    _ <- movies.take(5).traverse_(m => {
      val titleStr = m.title.getOrElse("N/A")
      val yearStr = m.release_date.getOrElse("N/A")
      val genresStr = m.genres.map(_.name).mkString(", ")
      IO.println(s"$titleStr - Año: $yearStr - Géneros: $genresStr")
    })
  } yield ()
}




