
import java.time.LocalDateTime

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.util.{Success, Try}

case class FieldContainer(fieldValue: Option[String], convertedValue: Try[Any], structField: StructField)

case class ConvertedLine(allFieldValid: Boolean, row: Option[Row], fieldContainerList: Option[List[FieldContainer]])

/**
* Converts an array of string to spark row , the result could be either a row or a error field list.
*/
class RowGenerator {

  def convertLineToRow(fieldValues: Array[String], structType: StructType, convertInvalidValueToNull: Boolean = false): ConvertedLine = {
    val structFields = structType.fields
    val fieldValuesOption = fieldValues.map(x => if (StringUtils.isBlank(x)) None else Some(x)).toList
    val diffCount = structFields.length - fieldValuesOption.size

    val fullFieldValuesOption: List[Option[String]] = if (diffCount > 0) {
      fieldValuesOption ::: List.fill[Option[String]](diffCount)(None)
    } else {
      fieldValuesOption
    }
    val fieldsValueStructList = fullFieldValuesOption.zip(structFields)
    val dformater = java.time.format.DateTimeFormatter.ofPattern("YYYY-MM-dd")
    val fieldContainerList = fieldsValueStructList.map(tp => {
      val structField = tp._2
      val strOpt = tp._1

      val tryValues = strOpt match {
        case None => Success(null)
        case Some(s) => Try(
          structField.dataType match {
            case IntegerType => s.toInt
            case LongType => s.toLong
            case FloatType => s.toFloat
            case DoubleType => s.toDouble
            case StringType => s
            case ByteType => s.toByte
            case DateType => java.time.LocalDate.parse(s, dformater)
            case TimestampType => LocalDateTime.parse(s, dformater)
            case _ => new RuntimeException("Unsupported type parsing csv:" + structField.dataType.typeName)
          }
        )
      }

      FieldContainer(strOpt, tryValues, structField)

    })

    val validLine: Boolean = fieldContainerList.forall(_.convertedValue.isSuccess)
    val errorFields = fieldContainerList.filter(_.convertedValue.isFailure)

    val convertedFieldContainerList = if (convertInvalidValueToNull && !validLine)
      fieldContainerList.map(f => FieldContainer(f.fieldValue, if (f.convertedValue.isFailure) Success(null) else f.convertedValue, f.structField))
    else fieldContainerList


    val rowOption = if (convertInvalidValueToNull || validLine) Some(Row.fromSeq(convertedFieldContainerList)) else None

    ConvertedLine(validLine, rowOption, Option(errorFields))


  }

}
