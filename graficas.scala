import cats.effect.IO
import com.github.tototoshi.csv.*
import org.nspl.*
import org.nspl.awtrenderer.*
import org.saddle.*
import cats.*
import doobie.*
import doobie.implicits.*
import java.io.File
import cats.effect.unsafe.implicits.global
import doobie.Transactor

object graficas {
  @main
  def integra() =

    val path2DataFilePartidos: String = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsPartidosYGoles.csv"
    val path2DataFile2Alineaciones: String = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\dsAlineacionesXTorneo.csv"

    val readerPartidos = CSVReader.open(new File(path2DataFilePartidos))
    val readerAlineaciones = CSVReader.open(new File(path2DataFile2Alineaciones))

    val contentFilePartidos: List[Map[String, String]] = readerPartidos.allWithHeaders()
    val contentFileAlineaciones: List[Map[String, String]] = readerAlineaciones.allWithHeaders()

    readerPartidos.close()
    readerAlineaciones.close()

    val rutaArchivo = "C:\\Users\\Usuario PC\\OneDrive\\Escritorio\\script\\graficos.png"

    val xa = Transactor.fromDriverManager[IO](
      driver = "com.mysql.cj.jdbc.Driver",
      url = "jdbc:mysql://localhost: 3306/mundiales",
      user = "root",
      password = "Mariangel19.",
      logHandler = None)

    val capacidadS: List[Int] = contentFilePartidos
      .flatMap(_.get("stadiums_stadium_capacity")
        .filter(_.forall(_.isDigit)).map(_.toInt))
    val histograma = xyplot(HistogramData(capacidadS, 20) -> bar())(
      xlab("Capacidad"),
      ylab("Frecuencia"),
      main("Capacidad de los estadios")
    )

