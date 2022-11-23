package cardan

import cats.effect.*
import org.typelevel.log4cats.*

trait LoggerFactory[F[_]] {
  def getLogger(name: String): SelfAwareStructuredLogger[F]
}

object LoggerFactory {
  def apply[F[_]: LoggerFactory]: LoggerFactory[F] = implicitly[LoggerFactory[F]]

  def default[F[_]: Async]: LoggerFactory[F] = new LoggerFactory[F] {
    override def getLogger(name: String): SelfAwareStructuredLogger[F] =
      slf4j.Slf4jLogger.getLoggerFromName[F](name)
  }
}
