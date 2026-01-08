package models

case class Crew (
                  credit_id: Option[String],
                  department: Option[String],
                  gender: Option[Int],
                  id: Option[Int],
                  job: Option[String],
                  name: Option[String],
                  profile_path: Option[String] 
                )
