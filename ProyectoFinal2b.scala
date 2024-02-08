import com.github.tototoshi.csv.*
import doobie.util.transactor

import java.io.File
import java.io.{BufferedWriter, FileWriter}

import doobie._
import doobie.implicits._

import cats._
import cats.effect._
import cats.effect.unsafe.IORuntime
import cats.effect.unsafe.implicits.global
import cats.implicits._

object proyectoFinal2b {
  @main
  def ej() =

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost: 3306/mundiales",
      user = "root",
      password = "Mariangel19.",
      logHandler = None)

    val ruta = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsAlineacionesXTorneo.csv"
    val reader = CSVReader.open(new File(ruta))
    val contentFile: List[Map[String, String]] =
      reader.allWithHeaders()

    val ruta2 = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsPartidosYGoles.csv"
    val reader2 = CSVReader.open(new File(ruta2))
    val contentFile2: List[Map[String, String]] =
      reader2.allWithHeaders()

    def valoresI(valor: String) =
      if (valor == "not available" || valor == "not applicable" || valor == "NA" || valor == "\\s") {
        0
      } else {
        valor.toDouble
      }

    def caracteres(valor: String) =
      val newValor = valor.replace("'", "\\'")
      newValor
    // -----------------------------------------------------------------------------------------------------------------

    def escribirtxt(nombre: String, archivo: String): Unit =
      val ruta = "C:/Users/Usuario PC/OneDrive/Escritorio/scripts/"
      val rutaF = ruta + nombre

      val escritor = new BufferedWriter(new FileWriter(rutaF, true))
      try {
        escritor.write(archivo)
        escritor.newLine()
      } finally {
        escritor.close()
      }

    def generateDataStadiumTableTXT(data: List[Map[String, String]]): Unit =
      val nombretxt = "stadiums.txt"
      val insertFormat = s"INSERT INTO stadiums(stadiums_stadium_name, stadiums_city_name, stadiums_country_name, " +
        s"stadiums_stadiums_capacity) VALUES('%s', '%s', '%s', %d);"
      val valores = data
        .map(x => (caracteres(x("stadiums_stadium_name").trim),
          caracteres(x("stadiums_city_name").trim),
          caracteres(x("stadiums_country_name").trim),
          valoresI(x("stadiums_stadium_capacity")).toInt
        ))
        .distinct
        .map(x => escribirtxt(nombretxt, insertFormat.formatLocal(java.util.Locale.US,
          x._1, x._2, x._3, x._4)))

    def generateDataTournamentsTableTXT(data: List[Map[String, String]]) =
      val nombretxt = "tournaments.txt"
      val insertFormat = s"INSERT INTO tournaments(tournaments_tournament_name,tournaments_year," +
        s"tournaments_host_country,tournaments_winner,tournaments_count_teams) VALUES('%s','%s', '%s', '%s', %d);"
      val valores = data
        .map(x => (caracteres(x("tournaments_tournament_name").trim),
          caracteres(x("tournaments_year").trim),
          caracteres(x("tournaments_host_country").trim),
          caracteres(x("tournaments_winner").trim),
          valoresI(x("tournaments_count_teams").trim).toInt))
        .distinct
        .map(x => escribirtxt(nombretxt, insertFormat.formatLocal(java.util.Locale.US,
          x._1, x._2, x._3, x._4, x._5)))

    def generateDataGoalsTXT(data: List[Map[String, String]]): Unit =
      val nombretxt = "goals.txt"
      val insertFormat = s"INSERT INTO goals(goals_goal_id, goals_team_id, goals_player_id, " +
        s"goals_player_team_id, goals_minute_label, goals_minute_regulation, goals_minute_stoppage," +
        s"goals_match_period,goals_own_goal , goals_penalty ) VALUES('%s', '%s', '%s', %s, '%s',%d,%d,'%s', %d, %d);"
      val value = data
        .map(x => (caracteres(x("goals_goal_id").trim),
          caracteres(x("goals_team_id").trim),
          caracteres(x("goals_player_id").trim),
          caracteres(x("goals_player_team_id").trim),
          caracteres(x("goals_minute_label").trim),
          valoresI(x("goals_minute_regulation").trim).toInt,
          valoresI(x("goals_minute_stoppage").trim).toInt,
          caracteres(x("goals_match_period").trim),
          valoresI(x("goals_own_goal").trim).toInt,
          valoresI(x("goals_penalty").trim).toInt
        ))
        .sortBy(x => (x._1, x._2))
        .map(x => escribirtxt(nombretxt, insertFormat.formatLocal(java.util.Locale.US,
          x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10)))

    def generateDataMatchesTableTXT(data: List[Map[String, String]]) =
      val nombretxt = "matches.txt"
      val insertFormat = s"INSERT INTO matches( matches_tournament_id, matches_match_id, matches_away_team_id," +
        s" matches_home_team_id, matches_stadium_id, matches_match_date,matches_match_time, matches_stage_name," +
        s" matches_home_team_score, matches_away_team_score,matches_extra_time, matches_penalty_shootout, " +
        s" matches_home_team_score_penalties,matches_away_team_score_penalties,matches_result) " +
        s"VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %d, %d, '%s');"

      val valores = data
        .map(x => (caracteres(x("matches_tournament_id")),
          caracteres(x("matches_match_id")),
          caracteres(x("matches_away_team_id")),
          caracteres(x("matches_home_team_id")),
          caracteres(x("matches_stadium_id")),
          caracteres(x("matches_match_date")),
          caracteres(x("matches_match_time")),
          caracteres(x("matches_stage_name")),
          valoresI(x("matches_home_team_score")).toInt,
          valoresI(x("matches_away_team_score")).toInt,
          valoresI(x("matches_extra_time")).toInt,
          valoresI(x("matches_penalty_shootout")).toInt,
          valoresI(x("matches_home_team_score_penalties")).toInt,
          valoresI(x("matches_away_team_score_penalties")).toInt,
          caracteres(x("matches_result"))
        ))
        .distinct
        .map(x => escribirtxt(nombretxt, insertFormat.formatLocal(java.util.Locale.US,
          x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, x._12, x._13, x._14, x._15)))
    //---------------------------------------------------------------------------------------------------------------------

    def generateDataStadiumTable(data: List[Map[String, String]]) =
      val v = data
        .map(x => (caracteres(x("stadiums_stadium_name").trim),
          caracteres(x("stadiums_city_name").trim),
          caracteres(x("stadiums_country_name").trim),
          valoresI(x("stadiums_stadium_capacity")).toInt
        ))
        .distinct
        .map(t =>
          sql"""INSERT INTO stadiums(stadiums_stadium_name, stadiums_city_name, stadiums_country_name,
               stadiums_stadiums_capacity) VALUES(${t._1},${t._2},${t._3},${t._4})"""
            .stripMargin
            .update)
      v

    def generateDataTournamentsTable(data: List[Map[String, String]]) =
      val v = data
        .map(x => (x("tournaments_tournament_name").trim,
          valoresI(x("tournaments_year").trim).toInt,
          x("tournaments_host_country").trim,
          x("tournaments_winner").trim,
          valoresI(x("tournaments_count_teams").trim).toInt))
        .distinct
        .map(t =>
          sql"""INSERT INTO tournaments(tournaments_tournament_name,tournaments_year,
              tournaments_host_country,tournaments_winner,tournaments_count_teams)
              VALUES(${t._1},${t._2},${t._3},${t._4},${t._5})"""
            .stripMargin
            .update)
      v

    def generateDataGoalsTable(data: List[Map[String, String]]) =
      val v = data
        .map(x => (caracteres(x("goals_goal_id").trim),
          caracteres(x("goals_team_id").trim),
          caracteres(x("goals_player_id").trim),
          caracteres(x("goals_player_team_id").trim),
          caracteres(x("goals_minute_label").trim),
          valoresI(x("goals_minute_regulation").trim).toInt,
          valoresI(x("goals_minute_stoppage").trim).toInt,
          caracteres(x("goals_match_period").trim),
          valoresI(x("goals_own_goal").trim).toInt,
          valoresI(x("goals_penalty").trim).toInt))
        .distinct
        .map(t =>
          sql"""INSERT INTO goals(goals_goal_id, goals_team_id, goals_player_id, goals_player_team_id,
              goals_minute_label, goals_minute_regulation, goals_minute_stoppage,
              goals_match_period,goals_own_goal , goals_penalty ) VALUES(${t._1}, ${t._2}, ${t._3}, ${t._4}, ${t._5},
              ${t._6}, ${t._7}, ${t._8}, ${t._9}, ${t._10})"""
            .stripMargin
            .update)
      v

  //generateDataStadiumTable(contentFile2).foreach(i => i.run.transact(xa).unsafeRunSync())
  //generateDataTournamentsTable(contentFile2).foreach(i => i.run.transact(xa).unsafeRunSync())
  //generateDataGoalsTable(contentFile2).foreach(i => i.run.transact(xa).unsafeRunSync())

  //generateDataTournamentsTableTXT(contentFile2)
  //generateDataStadiumTableTXT(contentFile2)
  //generateDataMatchesTableTXT(contentFile2)
  //generateDataGoalsTXT(contentFile2)


}
