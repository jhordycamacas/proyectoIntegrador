package problemas

import cats.effect.{IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.text
import fs2.data.csv.*
import fs2.data.csv.generic.semiauto.*
import scala.io.Source
import io.circe.*
import io.circe.parser.*
import io.circe.generic.auto.*
import io.circe.syntax.*
import models.Crew

object Avance2 extends App {

  val ruta = "src/main/resources/data/pi-movies-complete-2025-12-04.csv"

  val source = Source.fromFile(ruta, "UTF-8")
  val lines = source.getLines().toList
  source.close()

  val headers = lines.head.split(";").map(_.trim)
  val crewIndex = headers.indexOf("crew")

  if (crewIndex == -1) {
    println("La columna 'crew' no existe en el CSV")
    sys.exit(1)
  }

  def cleanCrewJson(crewJson: String): String = {
    crewJson
      .trim
      .replaceAll("'", "\"")
      .replaceAll("None", "null")
      .replaceAll("True", "true")
      .replaceAll("False", "false")
      .replaceAll("""\\""", "")
  }

  def normalizarTexto(texto: String): Option[String] = {
    val limpio = texto.trim.replaceAll("\\s+", " ")
    if (limpio.isEmpty) None else Some(limpio)
  }

  def normalizarEntero(valor: Int): Option[Int] =
    Some(if (valor >= 0) valor else 0)



  // Función para convertir campos vacíos a None (null en JSON)
  def limpiarCrew(crews: List[Crew]): List[Crew] = {
    crews
      .map(c => c.copy(
        credit_id = c.credit_id.flatMap(n => normalizarTexto(n)),
        department = c.department.flatMap(n => normalizarTexto(n)),
        gender = c.gender.flatMap(n => normalizarEntero(n)),
        id = c.id.flatMap(n => normalizarEntero(n)),
        job = c.job.flatMap(j => normalizarTexto(j)),
        name = c.name.flatMap(d => normalizarTexto(d)),
        profile_path = c.profile_path.flatMap(p => normalizarTexto(p))
      ))
      .distinct // Eliminar solo duplicados exactos
  }

  def parseCSVLine(line: String): Array[String] = {
    val (fields, lastBuilder, _) = line.foldLeft(
      (Vector.empty[String], new StringBuilder, false)
    ) {
      case ((fields, current, inQuotes), char) => char match {
        case '"' =>
          (fields, current, !inQuotes)

        case ';' if !inQuotes =>
          (fields :+ current.toString, new StringBuilder, false)

        case _ =>
          current.append(char)
          (fields, current, inQuotes)
      }
    }

    (fields :+ lastBuilder.toString).toArray
  }

  val listaCrew: List[Crew] = lines.tail.flatMap { line =>
    val parts = parseCSVLine(line)

    if (parts.length > crewIndex) {
      val crewStr = parts(crewIndex).trim

      if (crewStr.nonEmpty && crewStr != "[]") {
        try {
          val jsonLimpio = cleanCrewJson(crewStr)

          decode[List[Crew]](jsonLimpio) match {
            case Right(crews) => crews
            case Left(error) =>
              List.empty[Crew]  // Cambiado de None a List.empty
          }
        } catch {
          case e: Exception => List.empty[Crew]  // Cambiado de None a List.empty
        }
      } else {
        List.empty[Crew]  // Cambiado de None a List.empty
      }
    } else {
      List.empty[Crew]  // Cambiado de None a List.empty
    }
  }

  // Limpiar los datos (sin eliminar, solo normalizar)
  val crewLimpio = limpiarCrew(listaCrew)

  // Contar campos null
  val credit_idNull = crewLimpio.count(_.credit_id.isEmpty)
  val idNull = crewLimpio.count(_.id.isEmpty)
  val gendersNull = crewLimpio.count(_.gender.isEmpty)
  val nombresNull = crewLimpio.count(_.name.isEmpty)
  val deptosNull = crewLimpio.count(_.department.isEmpty)
  val jobsNull = crewLimpio.count(_.job.isEmpty)
  val profilesNull = crewLimpio.count(_.profile_path.isEmpty)

  println("=" * 60)
  println("RESUMEN DE PROCESAMIENTO CON CIRCE")
  println("=" * 60)
  println(s"Total de registros Crew procesados: ${crewLimpio.size}")
  println(s"\n Campos con valores null:")
  println(s"  - Credit_id null: $credit_idNull")
  println(s"  - Departamentos null: $deptosNull")
  println(s"  - Generos null: $gendersNull")
  println(s"  - ID null: $idNull")
  println(s"  - Jobs null: $jobsNull")
  println(s"  - Nombres null: $nombresNull")
  println(s"  - Profile paths null: $profilesNull")

  println("\n" + "=" * 60)
  println("PRIMEROS 5 REGISTROS (incluyendo nulls)")
  println("=" * 60)
  crewLimpio.take(5).foreach { crew =>
    println("\nMiembro del equipo:")
    println(s"  Credit_id: ${crew.credit_id.getOrElse("null")}")
    println(s"  Departamento: ${crew.department.getOrElse("null")}")
    println(s"  Genero: ${crew.gender.getOrElse("null")}")
    println(s"  Id: ${crew.id.getOrElse("null")}")
    println(s"  Trabajo: ${crew.job.getOrElse("null")}")
    println(s"  Nombre: ${crew.name.getOrElse("null")}")
    println(s"  Perfil: ${crew.profile_path.getOrElse("null")}")
  }

  // Estadísticas por departamento (excluyendo nulls para las estadísticas)
  println("\n" + "=" * 60)
  println("ESTADÍSTICAS POR DEPARTAMENTO (TOP 10)")
  println("=" * 60)
  crewLimpio
    .filter(_.department.isDefined) // Solo los que tienen departamento
    .groupBy(_.department.get)
    .map { case (dept, list) => (dept, list.size) }
    .toSeq
    .sortBy(-_._2)
    .take(10)
    .foreach { case (dept, count) =>
      println(f"  $dept%-30s: $count%,6d registros")
    }

  // Estadísticas por trabajo (excluyendo nulls)
  println("\n" + "=" * 60)
  println("TRABAJOS MÁS COMUNES (TOP 10)")
  println("=" * 60)
  crewLimpio
    .filter(_.job.isDefined) // Solo los que tienen trabajo
    .groupBy(_.job.get)
    .map { case (job, list) => (job, list.size) }
    .toSeq
    .sortBy(-_._2)
    .take(10)
    .foreach { case (job, count) =>
      println(f"  $job%-30s: $count%,6d registros")
    }

  // Exportar JSON limpio usando Circe (mostrará null donde corresponda)
  println("\n" + "=" * 60)
  println("JSON LIMPIO CON NULLS (Muestra de 3 registros)")
  println("=" * 60)
  val muestra = crewLimpio.take(3)
  println(muestra.asJson.spaces2)



  println("\n" + "=" * 60)
  println("PROCESAMIENTO COMPLETADO ✓")
  println("=" * 60)
}


