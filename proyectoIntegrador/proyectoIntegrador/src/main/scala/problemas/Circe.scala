package problemas

import cats.effect.{IO, IOApp}
import fs2.text
import fs2.io.file.{Files, Path}
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import io.circe.*
import io.circe.parser.*
import io.circe.generic.auto.*

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
case class Ratings (
                   userId: Int,
                   rating: Double,
                   timestamp: Long //Long por que el valor es grande
                   )

