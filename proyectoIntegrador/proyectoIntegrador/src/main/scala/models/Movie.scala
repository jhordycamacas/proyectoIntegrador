package models

case class Movie(
                  adult: Option[Boolean],
                  budget: Option[Double],
                  revenue: Option[Double],
                  popularity: Option[Double],
                  runtime: Option[Double],
                  vote_average: Option[Double],
                  vote_count: Option[Double],
                  original_language: Option[String],
                  status: Option[String]
                )
